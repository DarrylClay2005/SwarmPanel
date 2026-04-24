import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

import aiohttp
import aiomysql

from .bots import BOT_INDEX, MUSIC_BOTS, BotDefinition
from .config import Settings


logger = logging.getLogger("swarm_panel")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
SYSTEM_SCHEMAS = {"information_schema", "mysql", "performance_schema", "sys"}
YOUTUBE_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "music.youtube.com",
    "youtu.be",
    "www.youtu.be",
}
SOUNDCLOUD_HOST_SUFFIXES = ("soundcloud.com",)
SOUNDCLOUD_HOSTS = {"snd.sc"}
THUMBNAIL_CACHE_TTL_SECONDS = 60 * 60
GUILD_SETTINGS_COLUMNS = (
    ("home_vc_id", "BIGINT"),
    ("volume", "INT DEFAULT 100"),
    ("loop_mode", "VARCHAR(10) DEFAULT 'queue'"),
    ("filter_mode", "VARCHAR(20) DEFAULT 'none'"),
    ("dj_role_id", "BIGINT DEFAULT NULL"),
    ("feedback_channel_id", "BIGINT DEFAULT NULL"),
    ("transition_mode", "VARCHAR(10) DEFAULT 'off'"),
    ("custom_speed", "FLOAT DEFAULT 1.0"),
    ("custom_pitch", "FLOAT DEFAULT 1.0"),
    ("custom_modifiers_left", "INT DEFAULT 0"),
    ("dj_only_mode", "BOOLEAN DEFAULT FALSE"),
    ("stay_in_vc", "BOOLEAN DEFAULT FALSE"),
)


def _validate_identifier(value: str, field_name: str) -> str:
    value = (value or "").strip()
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Invalid {field_name}: {value!r}")
    return value


def _coerce_int(value: Any, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {field_name}: {value!r}") from None


def _normalize_loop_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    valid_modes = {"off", "song", "queue"}
    if mode not in valid_modes:
        raise ValueError(f"Invalid loop mode: {value!r}. Expected one of: {', '.join(sorted(valid_modes))}")
    return mode


def _normalize_filter_mode(value: Any) -> str:
    mode = str(value or "").strip().lower().replace(" ", "")
    valid_modes = {"none", "nightcore", "vaporwave", "bassboost", "8d"}
    if mode not in valid_modes:
        raise ValueError(f"Invalid filter mode: {value!r}. Expected one of: {', '.join(sorted(valid_modes))}")
    return mode


def _derive_session_state(
    playback: dict[str, Any],
    *,
    queue_count: int,
    has_settings: bool,
    home_channel_id: int | None,
    backup_queue_count: int = 0,
) -> tuple[str, str]:
    is_playing = bool(playback.get("is_playing"))
    is_paused = bool(playback.get("is_paused"))
    has_track = bool(playback.get("title") or playback.get("video_url"))
    has_channel = playback.get("channel_id") is not None
    has_recovery_path = bool(home_channel_id and (has_track or queue_count > 0 or backup_queue_count > 0))

    if is_paused and has_track and has_channel:
        return "paused", "Paused"
    if is_playing and has_track and has_channel:
        return "playing", "Playing"
    if has_track and has_channel:
        return "paused", "Paused"
    if has_recovery_path and (queue_count > 0 or backup_queue_count > 0 or has_track):
        return "recovering", "Recovery Pending"
    if queue_count > 0:
        return "queued", "Queued"
    if has_settings or home_channel_id:
        return "configured", "Configured"
    return "idle", "Idle"



def _extract_youtube_video_id(video_url: str | None) -> str | None:
    if not video_url:
        return None

    try:
        parsed = urlparse(video_url.strip())
    except Exception:
        return None

    host = (parsed.netloc or "").lower()
    if host not in YOUTUBE_HOSTS:
        return None

    if host.endswith("youtu.be"):
        video_id = parsed.path.strip("/").split("/", 1)[0]
        return video_id or None

    path_parts = [part for part in parsed.path.split("/") if part]
    if parsed.path == "/watch":
        video_id = parse_qs(parsed.query).get("v", [None])[0]
        return video_id or None

    if len(path_parts) >= 2 and path_parts[0] in {"shorts", "embed", "live"}:
        return path_parts[1] or None

    return None


def _is_soundcloud_url(video_url: str | None) -> bool:
    if not video_url:
        return False

    try:
        parsed = urlparse(video_url.strip())
    except Exception:
        return False

    host = (parsed.netloc or "").lower()
    return host in SOUNDCLOUD_HOSTS or host.endswith(SOUNDCLOUD_HOST_SUFFIXES)


def _is_generic_url(video_url: str | None) -> bool:
    if not video_url:
        return False

    try:
        parsed = urlparse(video_url.strip())
    except Exception:
        return False

    return bool(parsed.scheme and parsed.netloc)


def _detect_media_source(video_url: str | None) -> dict[str, str]:
    if _extract_youtube_video_id(video_url):
        return {"key": "youtube", "label": "YouTube"}
    if _is_soundcloud_url(video_url):
        return {"key": "soundcloud", "label": "SoundCloud"}
    if _is_generic_url(video_url):
        return {"key": "link", "label": "Direct Link"}
    if str(video_url or "").strip():
        return {"key": "search", "label": "Search"}
    return {"key": "unknown", "label": "Unknown"}


def _derive_thumbnail_url(video_url: str | None) -> str | None:
    video_id = _extract_youtube_video_id(video_url)
    if not video_id:
        return None
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


class PanelDatabase:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: aiomysql.Pool | None = None
        self.http_session: aiohttp.ClientSession | None = None
        self.thumbnail_cache: dict[str, tuple[float, str | None]] = {}

    async def connect(self) -> None:
        if not self.pool:
            self.pool = await aiomysql.create_pool(
                host=self.settings.db_host,
                port=self.settings.db_port,
                user=self.settings.db_user,
                password=self.settings.db_password,
                db=self.settings.db_default_schema,
                autocommit=True,
                minsize=1,
                maxsize=10,
                connect_timeout=10,
            )
        if not self.http_session:
            timeout = aiohttp.ClientTimeout(total=10)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
        if self.http_session:
            await self.http_session.close()
            self.http_session = None

    async def _fetchall(self, query: str, params: tuple[Any, ...] = (), dict_cursor: bool = True):
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        async with self.pool.acquire() as conn:
            cursor_cls = aiomysql.DictCursor if dict_cursor else None
            async with conn.cursor(cursor_cls) as cur:
                await asyncio.wait_for(cur.execute(query, params), timeout=15)
                return await asyncio.wait_for(cur.fetchall(), timeout=15)

    async def _fetchone(self, query: str, params: tuple[Any, ...] = (), dict_cursor: bool = True):
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        async with self.pool.acquire() as conn:
            cursor_cls = aiomysql.DictCursor if dict_cursor else None
            async with conn.cursor(cursor_cls) as cur:
                await asyncio.wait_for(cur.execute(query, params), timeout=15)
                return await asyncio.wait_for(cur.fetchone(), timeout=15)

    async def _execute(self, query: str, params: tuple[Any, ...] = ()) -> int:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await asyncio.wait_for(cur.execute(query, params), timeout=15)
                return cur.rowcount

    async def _ensure_music_guild_settings_schema(self, cur, schema: str, prefix: str) -> None:
        schema = _validate_identifier(schema, "schema")
        prefix = _validate_identifier(prefix, "table prefix")
        await asyncio.wait_for(cur.execute(
            f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_guild_settings` "
            "(guild_id BIGINT PRIMARY KEY)"
        ), timeout=15)
        for column_name, definition in GUILD_SETTINGS_COLUMNS:
            try:
                safe_column = _validate_identifier(column_name, "column name")
                await asyncio.wait_for(cur.execute(
                    f"ALTER TABLE `{schema}`.`{prefix}_guild_settings` "
                    f"ADD COLUMN {safe_column} {definition}"
                ), timeout=15)
            except Exception:
                pass

    async def _resolve_soundcloud_thumbnail(self, video_url: str | None) -> str | None:
        if not _is_soundcloud_url(video_url):
            return None

        normalized_url = str(video_url or "").strip()
        if not normalized_url:
            return None

        now = time.time()
        cached = self.thumbnail_cache.get(normalized_url)
        if cached and cached[0] > now:
            return cached[1]

        thumbnail: str | None = None
        if self.http_session:
            try:
                async with self.http_session.get(
                    "https://soundcloud.com/oembed",
                    params={"format": "json", "url": normalized_url},
                ) as resp:
                    if resp.ok:
                        payload = await resp.json()
                        raw_thumbnail = payload.get("thumbnail_url")
                        if raw_thumbnail:
                            thumbnail = str(raw_thumbnail).strip() or None
            except Exception:
                thumbnail = None

        self.thumbnail_cache[normalized_url] = (now + THUMBNAIL_CACHE_TTL_SECONDS, thumbnail)
        return thumbnail

    async def _get_thumbnail_url(self, video_url: str | None) -> str | None:
        youtube_thumbnail = _derive_thumbnail_url(video_url)
        if youtube_thumbnail:
            return youtube_thumbnail
        return await self._resolve_soundcloud_thumbnail(video_url)

    async def _table_exists(self, schema: str, table: str) -> bool:
        row = await self._fetchone(
            """
            SELECT 1 AS table_exists
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        return bool(row)

    async def ping(self) -> bool:
        try:
            row = await self._fetchone("SELECT 1 AS ok")
            return bool(row and row.get("ok") == 1)
        except Exception:
            return False

    async def list_schemas(self) -> list[str]:
        rows = await self._fetchall(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY schema_name
            """
        )
        return [row["schema_name"] for row in rows]

    async def list_tables(self, schema: str) -> list[dict[str, Any]]:
        schema = _validate_identifier(schema, "schema")
        rows = await self._fetchall(
            """
            SELECT table_name, table_rows
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (schema,),
        )
        return [
            {"table_name": row["table_name"], "estimated_rows": int(row["table_rows"] or 0)}
            for row in rows
        ]

    async def get_known_guild_ids(self, bot_key: str) -> list[int]:
        bot = BOT_INDEX.get(bot_key)
        if not bot or bot.kind != "music" or not bot.db_schema or not bot.table_prefix:
            return []

        schema = _validate_identifier(bot.db_schema, "schema")
        prefix = _validate_identifier(bot.table_prefix, "table prefix")
        tables = [f"{prefix}_playback_state", f"{prefix}_guild_settings", f"{prefix}_queue", f"{prefix}_bot_home_channels"]
        guild_ids: set[int] = set()

        for table in tables:
            if not await self._table_exists(schema, table):
                continue
            rows = await self._fetchall(f"SELECT DISTINCT guild_id FROM `{schema}`.`{table}`")
            for row in rows:
                guild_id = row.get("guild_id")
                if guild_id is not None:
                    guild_ids.add(int(guild_id))

        return sorted(guild_ids)

    async def get_bot_control_state(self, bot_key: str, guild_id: str | int) -> dict[str, Any]:
        bot = BOT_INDEX.get(bot_key)
        if not bot or bot.kind != "music" or not bot.db_schema or not bot.table_prefix:
            raise ValueError("This action is only supported for music bots")

        gid = _coerce_int(guild_id, "guild_id")
        schema = _validate_identifier(bot.db_schema, "schema")
        prefix = _validate_identifier(bot.table_prefix, "table prefix")
        playback_table = f"{prefix}_playback_state"
        settings_table = f"{prefix}_guild_settings"
        queue_table = f"{prefix}_queue"
        backup_table = f"{prefix}_queue_backup"
        home_table = f"{prefix}_bot_home_channels"
        direct_orders_table = f"{prefix}_swarm_direct_orders"
        heartbeat_table = "swarm_health"

        table_exists = {
            "playback": await self._table_exists(schema, playback_table),
            "settings": await self._table_exists(schema, settings_table),
            "queue": await self._table_exists(schema, queue_table),
            "backup": await self._table_exists(schema, backup_table),
            "home": await self._table_exists(schema, home_table),
            "direct_orders": await self._table_exists(schema, direct_orders_table),
            "heartbeat": await self._table_exists(schema, heartbeat_table),
        }

        playback: dict[str, Any] = {}
        settings: dict[str, Any] = {}
        queue_count = 0
        backup_queue_count = 0
        pending_direct_orders = 0
        latest_direct_order: dict[str, Any] | None = None
        home_channel_id: int | None = None
        feedback_channel_id: int | None = None
        heartbeat_age = None
        heartbeat_status = "unknown"

        if table_exists["playback"]:
            try:
                playback = await self._fetchone(
                    f"SELECT * "
                    f"FROM `{schema}`.`{playback_table}` WHERE guild_id = %s AND bot_name = %s LIMIT 1",
                    (gid, bot.key),
                ) or {}
            except Exception:
                playback = await self._fetchone(
                    f"SELECT * "
                    f"FROM `{schema}`.`{playback_table}` WHERE guild_id = %s LIMIT 1",
                    (gid,),
                ) or {}

        if table_exists["settings"]:
            settings = await self._fetchone(
                f"SELECT volume, loop_mode, filter_mode, feedback_channel_id, transition_mode, "
                f"custom_speed, custom_pitch, custom_modifiers_left, dj_only_mode, stay_in_vc "
                f"FROM `{schema}`.`{settings_table}` WHERE guild_id = %s LIMIT 1",
                (gid,),
            ) or {}
            feedback_channel_id = int(settings["feedback_channel_id"]) if settings.get("feedback_channel_id") else None

        if table_exists["queue"]:
            try:
                row = await self._fetchone(
                    f"SELECT COUNT(*) AS queue_count FROM `{schema}`.`{queue_table}` WHERE guild_id = %s AND bot_name = %s",
                    (gid, bot.key),
                ) or {}
            except Exception:
                row = await self._fetchone(
                    f"SELECT COUNT(*) AS queue_count FROM `{schema}`.`{queue_table}` WHERE guild_id = %s",
                    (gid,),
                ) or {}
            queue_count = int(row.get("queue_count") or 0)

        if table_exists["backup"]:
            try:
                row = await self._fetchone(
                    f"SELECT COUNT(*) AS backup_queue_count FROM `{schema}`.`{backup_table}` WHERE guild_id = %s AND bot_name = %s",
                    (gid, bot.key),
                ) or {}
            except Exception:
                row = await self._fetchone(
                    f"SELECT COUNT(*) AS backup_queue_count FROM `{schema}`.`{backup_table}` WHERE guild_id = %s",
                    (gid,),
                ) or {}
            backup_queue_count = int(row.get("backup_queue_count") or 0)

        if table_exists["home"]:
            try:
                row = await self._fetchone(
                    f"SELECT home_vc_id FROM `{schema}`.`{home_table}` WHERE guild_id = %s AND bot_name = %s LIMIT 1",
                    (gid, bot.key),
                ) or {}
            except Exception:
                row = await self._fetchone(
                    f"SELECT home_vc_id FROM `{schema}`.`{home_table}` WHERE guild_id = %s LIMIT 1",
                    (gid,),
                ) or {}
            home_channel_id = int(row["home_vc_id"]) if row.get("home_vc_id") else None

        if table_exists["direct_orders"]:
            try:
                row = await self._fetchone(
                    f"SELECT COUNT(*) AS pending_direct_orders FROM `{schema}`.`{direct_orders_table}` WHERE guild_id = %s AND bot_name = %s",
                    (gid, bot.key),
                ) or {}
            except Exception:
                row = await self._fetchone(
                    f"SELECT COUNT(*) AS pending_direct_orders FROM `{schema}`.`{direct_orders_table}` WHERE guild_id = %s",
                    (gid,),
                ) or {}
            pending_direct_orders = int(row.get("pending_direct_orders") or 0)
            try:
                latest_direct_order = await self._fetchone(
                    f"SELECT command, data, vc_id, text_channel_id "
                    f"FROM `{schema}`.`{direct_orders_table}` WHERE guild_id = %s AND bot_name = %s "
                    f"ORDER BY id DESC LIMIT 1",
                    (gid, bot.key),
                ) or None
            except Exception:
                try:
                    latest_direct_order = await self._fetchone(
                        f"SELECT command, data, vc_id, text_channel_id "
                        f"FROM `{schema}`.`{direct_orders_table}` WHERE guild_id = %s "
                        f"ORDER BY id DESC LIMIT 1",
                        (gid,),
                    ) or None
                except Exception:
                    latest_direct_order = await self._fetchone(
                        f"SELECT command, data, vc_id "
                        f"FROM `{schema}`.`{direct_orders_table}` WHERE guild_id = %s "
                        f"ORDER BY id DESC LIMIT 1",
                        (gid,),
                    ) or None

        if table_exists["heartbeat"]:
            row = await self._fetchone(
                f"SELECT status, TIMESTAMPDIFF(SECOND, last_pulse, NOW()) AS heartbeat_age "
                f"FROM `{schema}`.`{heartbeat_table}` WHERE bot_name = %s LIMIT 1",
                (bot.key,),
            ) or {}
            if row:
                heartbeat_age = int(row.get("heartbeat_age") or 0)
                heartbeat_status = row.get("status") or "unknown"

        session_state, session_state_label = _derive_session_state(
            playback,
            queue_count=queue_count,
            has_settings=bool(settings),
            home_channel_id=home_channel_id,
            backup_queue_count=backup_queue_count,
        )

        backup_restore_ready = bool(
            backup_queue_count > 0
            and home_channel_id
            and (
                queue_count == 0
                or not bool(playback.get("is_playing"))
                or session_state in {"recovering", "configured", "idle"}
            )
        )
        if backup_queue_count <= 0:
            backup_restore_reason = "No backup queue entries are stored for this guild."
        elif not home_channel_id:
            backup_restore_reason = "Backup queue exists, but no home channel is set for auto-restore."
        elif bool(playback.get("is_playing")) and queue_count > 0:
            backup_restore_reason = "Live playback/queue is already active, so backup restore is standing by."
        elif queue_count > 0:
            backup_restore_reason = "Live queue already contains items, so backup restore is waiting for an empty queue."
        else:
            backup_restore_reason = "Backup queue is armed and should restore this guild automatically if playback stalls."

        return {
            "key": bot.key,
            "display_name": bot.display_name,
            "guild_id": str(gid),
            "db": {
                "status": "online",
                "reachable": True,
                "message": "Live bot schema query succeeded.",
                "schema": schema,
            },
            "heartbeat": {
                "status": heartbeat_status,
                "age_seconds": heartbeat_age,
            },
            "session": {
                "guild_id": str(gid),
                "guild_name": None,
                "channel_id": str(playback.get("channel_id")) if playback.get("channel_id") else None,
                "channel_name": None,
                "title": playback.get("title"),
                "video_url": playback.get("video_url"),
                "position_seconds": int(playback.get("position_seconds") or 0),
                "is_playing": bool(playback.get("is_playing")),
                "session_state": session_state,
                "session_state_label": session_state_label,
                "volume": int(settings.get("volume") or 100),
                "loop_mode": settings.get("loop_mode") or "queue",
                "filter_mode": settings.get("filter_mode") or "none",
                "transition_mode": settings.get("transition_mode") or "off",
                "custom_speed": float(settings.get("custom_speed") or 1.0),
                "custom_pitch": float(settings.get("custom_pitch") or 1.0),
                "custom_modifiers_left": int(settings.get("custom_modifiers_left") or 0),
                "dj_only_mode": bool(settings.get("dj_only_mode")),
                "stay_in_vc": bool(settings.get("stay_in_vc")),
                "queue_count": queue_count,
                "backup_queue_count": backup_queue_count,
                "backup_restore_ready": backup_restore_ready,
                "backup_restore_reason": backup_restore_reason,
                "pending_direct_orders": pending_direct_orders,
                "latest_direct_order": latest_direct_order,
                "home_channel_id": str(home_channel_id) if home_channel_id else None,
                "home_channel_name": None,
                "feedback_channel_id": str(feedback_channel_id) if feedback_channel_id else None,
                "feedback_channel_name": None,
            },
        }

    async def _music_bot_snapshot(self, bot: BotDefinition) -> dict[str, Any]:
        assert bot.db_schema and bot.table_prefix
        schema = _validate_identifier(bot.db_schema, "schema")
        prefix = _validate_identifier(bot.table_prefix, "table prefix")
        playback_table = f"{prefix}_playback_state"
        settings_table = f"{prefix}_guild_settings"
        queue_table = f"{prefix}_queue"
        backup_table = f"{prefix}_queue_backup"
        home_table = f"{prefix}_bot_home_channels"
        heartbeat_table = "swarm_health"

        table_exists = {
            "playback": await self._table_exists(schema, playback_table),
            "settings": await self._table_exists(schema, settings_table),
            "queue": await self._table_exists(schema, queue_table),
            "backup": await self._table_exists(schema, backup_table),
            "home": await self._table_exists(schema, home_table),
            "heartbeat": await self._table_exists(schema, heartbeat_table),
        }

        playback_rows: list[dict[str, Any]] = []
        filter_map: dict[int, dict[str, Any]] = {}
        queue_map: dict[int, int] = {}
        backup_queue_map: dict[int, int] = {}
        home_map: dict[int, int | None] = {}
        known_guilds: set[int] = set()

        if table_exists["playback"]:
            try:
                playback_rows = await self._fetchall(
                    f"SELECT * FROM `{schema}`.`{playback_table}` ORDER BY guild_id"
                )
            except Exception:
                playback_rows = []
            for row in playback_rows:
                guild_id = row.get("guild_id")
                if guild_id is not None:
                    known_guilds.add(int(guild_id))

        if table_exists["settings"]:
            rows = await self._fetchall(f"SELECT * FROM `{schema}`.`{settings_table}`")
            for row in rows:
                guild_id = int(row["guild_id"])
                filter_map[guild_id] = {
                    "filter_mode": row.get("filter_mode") or "none",
                    "loop_mode": row.get("loop_mode") or "queue",
                }
                known_guilds.add(guild_id)

        if table_exists["queue"]:
            try:
                rows = await self._fetchall(
                    f"SELECT guild_id, COUNT(*) AS queue_len FROM `{schema}`.`{queue_table}` WHERE bot_name = %s GROUP BY guild_id",
                    (bot.key,),
                )
            except Exception:
                rows = await self._fetchall(
                    f"SELECT guild_id, COUNT(*) AS queue_len FROM `{schema}`.`{queue_table}` GROUP BY guild_id"
                )
            for row in rows:
                guild_id = int(row["guild_id"])
                queue_map[guild_id] = int(row.get("queue_len") or 0)
                known_guilds.add(guild_id)

        if table_exists["backup"]:
            try:
                rows = await self._fetchall(
                    f"SELECT guild_id, COUNT(*) AS backup_len FROM `{schema}`.`{backup_table}` WHERE bot_name = %s GROUP BY guild_id",
                    (bot.key,),
                )
            except Exception:
                rows = await self._fetchall(
                    f"SELECT guild_id, COUNT(*) AS backup_len FROM `{schema}`.`{backup_table}` GROUP BY guild_id"
                )
            for row in rows:
                guild_id = int(row["guild_id"])
                backup_queue_map[guild_id] = int(row.get("backup_len") or 0)

        if table_exists["home"]:
            rows = await self._fetchall(
                f"SELECT guild_id, home_vc_id FROM `{schema}`.`{home_table}` WHERE bot_name = %s",
                (bot.key,),
            )
            for row in rows:
                guild_id = int(row["guild_id"])
                home_map[guild_id] = int(row["home_vc_id"]) if row.get("home_vc_id") else None
                known_guilds.add(guild_id)

        playback_map = {int(row["guild_id"]): row for row in playback_rows if row.get("guild_id") is not None}
        sessions = []
        active_playing_count = 0
        sorted_guild_ids = sorted(known_guilds)

        for guild_id in sorted_guild_ids:
            playback = playback_map.get(guild_id, {})
            settings = filter_map.get(guild_id, {})
            queue_count = queue_map.get(guild_id, 0)
            backup_queue_count = backup_queue_map.get(guild_id, 0)
            home_channel_id = home_map.get(guild_id)
            source_info = _detect_media_source(playback.get("video_url"))
            is_playing = bool(playback.get("is_playing"))
            session_state, session_state_label = _derive_session_state(
                playback,
                queue_count=queue_count,
                has_settings=guild_id in filter_map,
                home_channel_id=home_channel_id,
                backup_queue_count=backup_queue_count,
            )
            if is_playing:
                active_playing_count += 1
            sessions.append(
                {
                    "guild_id": str(guild_id),
                    "channel_id": str(playback.get("channel_id")) if playback.get("channel_id") else None,
                    "title": playback.get("title"),
                    "video_url": playback.get("video_url"),
                    "media_source": source_info["key"],
                    "media_source_label": source_info["label"],
                    "thumbnail": None,
                    "position_seconds": int(playback.get("position_seconds") or 0),
                    "is_playing": is_playing,
                    "session_state": session_state,
                    "session_state_label": session_state_label,
                    "filter_mode": settings.get("filter_mode", "none"),
                    "loop_mode": settings.get("loop_mode", "queue"),
                    "queue_count": queue_count,
                    "backup_queue_count": backup_queue_count,
                    "backup_restore_ready": bool(backup_queue_count > 0 and session_state in {"recovering", "queued", "configured", "idle", "paused"}),
                    "backup_restore_reason": "Backup queue is armed when the live queue or playback path goes idle." if backup_queue_count > 0 else "No backup queue entries are stored for this guild.",
                    "home_channel_id": str(home_channel_id) if home_channel_id else None,
                    "home_channel_name": None,
                    "guild_name": None,
                    "channel_name": None,
                }
            )

        if sessions:
            thumbnails = await asyncio.gather(
                *(self._get_thumbnail_url(session.get("video_url")) for session in sessions)
            )
            for session, thumbnail in zip(sessions, thumbnails):
                session["thumbnail"] = thumbnail

        heartbeat_age = None
        heartbeat_status = "unknown"
        if table_exists["heartbeat"]:
            row = await self._fetchone(
                f"SELECT status, TIMESTAMPDIFF(SECOND, last_pulse, NOW()) AS heartbeat_age FROM `{schema}`.`{heartbeat_table}` WHERE bot_name = %s LIMIT 1",
                (bot.key,),
            )
            if row:
                heartbeat_age = int(row.get("heartbeat_age") or 0)
                heartbeat_status = row.get("status") or "unknown"

        node_status = "unknown" if heartbeat_age is None else ("online" if heartbeat_age <= 120 else "stale")

        return {
            "key": bot.key,
            "display_name": bot.display_name,
            "kind": bot.kind,
            "schema": schema,
            "status": node_status,
            "heartbeat_age_seconds": heartbeat_age,
            "heartbeat_status": heartbeat_status,
            "active_playing_count": active_playing_count,
            "known_guild_count": len(known_guilds),
            "sessions": sessions,
        }

    async def get_dashboard_data(self) -> dict[str, Any]:
        bots = []
        for bot in MUSIC_BOTS:
            try:
                bots.append(await self._music_bot_snapshot(bot))
            except Exception as exc:
                logger.exception("Failed collecting snapshot for %s", bot.key)
                bots.append(
                    {
                        "key": bot.key,
                        "display_name": bot.display_name,
                        "kind": bot.kind,
                        "schema": bot.db_schema,
                        "status": "error",
                        "error": str(exc),
                        "heartbeat_age_seconds": None,
                        "heartbeat_status": "unknown",
                        "active_playing_count": 0,
                        "known_guild_count": 0,
                        "sessions": [],
                    }
                )

        total_active = sum(int(bot.get("active_playing_count") or 0) for bot in bots)
        aria_status = "online" if await self.ping() else "error"
        aria_recent_interactions: list[dict[str, Any]] = []
        aria_recent_interaction_count = 0
        aria_medic_summary: dict[str, Any] = {
            "pending_repairs": 0,
            "pending_infra": 0,
            "critical_health": 0,
            "recoverable_health": 0,
            "recent_operator_decisions": [],
            "recent_infra_history": [],
            "recent_swarm_events": [],
        }
        # Authentic Database Query for Aria
        aria_heartbeat_age = None
        aria_heartbeat_status = "n/a"
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT status, TIMESTAMPDIFF(SECOND, last_pulse, NOW()) as age FROM `discord_aria`.`swarm_health` WHERE bot_name = 'aria'")
                    row = await cur.fetchone()
                    if row:
                        aria_heartbeat_age = row["age"]
                        aria_heartbeat_status = row["status"]
                    try:
                        await cur.execute(
                            """
                            CREATE TABLE IF NOT EXISTS `discord_aria`.`aria_interactions` (
                                id INT AUTO_INCREMENT PRIMARY KEY,
                                guild_id BIGINT NULL,
                                channel_id BIGINT NULL,
                                user_id BIGINT NULL,
                                user_name VARCHAR(150) NULL,
                                interaction_type VARCHAR(32) NOT NULL DEFAULT 'chat',
                                prompt_text TEXT,
                                response_text TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                            """
                        )
                        await cur.execute(
                            "SELECT COUNT(*) AS total FROM `discord_aria`.`aria_interactions`"
                        )
                        count_row = await cur.fetchone() or {}
                        aria_recent_interaction_count = int(count_row.get("total") or 0)
                        await cur.execute(
                            """
                            SELECT guild_id, channel_id, user_id, user_name, interaction_type, prompt_text, response_text, created_at
                            FROM `discord_aria`.`aria_interactions`
                            ORDER BY created_at DESC, id DESC
                            LIMIT 6
                            """
                        )
                        aria_recent_interactions = list(await cur.fetchall() or [])
                    except Exception:
                        aria_recent_interactions = []
                        aria_recent_interaction_count = 0
                    try:
                        await cur.execute("SELECT COUNT(*) AS total FROM `discord_aria`.`aria_repair_tasks` WHERE status='pending'")
                        row = await cur.fetchone() or {}
                        aria_medic_summary["pending_repairs"] = int(row.get("total") or 0)
                    except Exception:
                        pass
                    try:
                        await cur.execute("SELECT COUNT(*) AS total FROM `discord_aria`.`aria_infra_tasks` WHERE status='pending'")
                        row = await cur.fetchone() or {}
                        aria_medic_summary["pending_infra"] = int(row.get("total") or 0)
                    except Exception:
                        pass
                    try:
                        await cur.execute("SELECT COUNT(*) AS total FROM `discord_aria`.`aria_swarm_health` WHERE status_label='critical'")
                        row = await cur.fetchone() or {}
                        aria_medic_summary["critical_health"] = int(row.get("total") or 0)
                    except Exception:
                        pass
                    try:
                        await cur.execute("SELECT COUNT(*) AS total FROM `discord_aria`.`aria_swarm_health` WHERE status_label IN ('recoverable','degraded')")
                        row = await cur.fetchone() or {}
                        aria_medic_summary["recoverable_health"] = int(row.get("total") or 0)
                    except Exception:
                        pass
                    try:
                        await cur.execute("SELECT issue_type, bot_name, guild_id, priority_score, urgency_label, created_at FROM `discord_aria`.`aria_operator_decisions` ORDER BY created_at DESC, id DESC LIMIT 5")
                        aria_medic_summary["recent_operator_decisions"] = list(await cur.fetchall() or [])
                    except Exception:
                        aria_medic_summary["recent_operator_decisions"] = []
                    try:
                        await cur.execute("SELECT target_name, action_name, issue_type, success, execution_mode, result_text, created_at FROM `discord_aria`.`aria_infra_history` ORDER BY created_at DESC, id DESC LIMIT 5")
                        aria_medic_summary["recent_infra_history"] = list(await cur.fetchall() or [])
                    except Exception:
                        aria_medic_summary["recent_infra_history"] = []
                    try:
                        await cur.execute("SELECT event_type, bot_name, guild_id, severity, created_at FROM `discord_aria`.`aria_swarm_events` ORDER BY created_at DESC, id DESC LIMIT 6")
                        aria_medic_summary["recent_swarm_events"] = list(await cur.fetchall() or [])
                    except Exception:
                        aria_medic_summary["recent_swarm_events"] = []
        except Exception:
            pass

        # Aria is ONLINE only when it has a recent heartbeat.
        # Never infer ONLINE from the presence of music bots — that masks an offline Aria.
        if aria_heartbeat_age is not None and aria_heartbeat_age < 120:
            aria_status_real = "ONLINE"
        else:
            aria_status_real = "OFFLINE"

        bots.append(
            {
                "key": "aria",
                "display_name": "Aria",
                "kind": "orchestrator",
                "schema": "discord_aria",
                "status": aria_status_real,
                "heartbeat_age_seconds": aria_heartbeat_age,
                "heartbeat_status": aria_heartbeat_status,
                "active_playing_count": total_active,
                "known_guild_count": sum(int(bot.get("known_guild_count") or 0) for bot in bots),
                "sessions": [],
                "recent_interactions": aria_recent_interactions,
                "recent_interaction_count": aria_recent_interaction_count,
                "medic_summary": aria_medic_summary,
            }
        )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "bots": bots,
        }



    async def get_metrics_snapshot(self) -> dict[str, Any]:
        """Aggregate bot-written voice persistence and runtime metrics for the panel."""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        bots: list[dict[str, Any]] = []
        totals = {
            "bots": 0,
            "guilds": 0,
            "voice_connected": 0,
            "playing": 0,
            "paused": 0,
            "queued_tracks": 0,
            "backup_tracks": 0,
            "recovering": 0,
            "lavalink_ready": 0,
            "stale_metrics": 0,
        }

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                for bot in MUSIC_BOTS:
                    schema = bot.db_schema
                    prefix = bot.table_prefix
                    bot_metrics: list[dict[str, Any]] = []
                    error: str | None = None
                    try:
                        await cur.execute(
                            f"""
                            SELECT
                                m.guild_id,
                                m.voice_connected,
                                m.connected_channel_id,
                                m.player_connected,
                                m.player_playing,
                                m.player_paused,
                                m.queue_count,
                                m.backup_queue_count,
                                m.is_playing_db,
                                m.is_paused_db,
                                m.position_seconds,
                                m.recovery_pending,
                                m.lavalink_ready,
                                m.last_error,
                                TIMESTAMPDIFF(SECOND, m.updated_at, NOW()) AS metrics_age_seconds,
                                v.last_channel_id,
                                v.connected_channel_id AS voice_state_connected_channel_id,
                                v.desired_connected,
                                v.reconnect_attempts,
                                v.last_error AS voice_last_error,
                                TIMESTAMPDIFF(SECOND, v.last_seen_at, NOW()) AS voice_age_seconds
                            FROM `{schema}`.`{prefix}_metrics` m
                            LEFT JOIN `{schema}`.`{prefix}_voice_state` v
                              ON v.guild_id = m.guild_id AND v.bot_name = m.bot_name
                            WHERE m.bot_name = %s
                            ORDER BY m.updated_at DESC
                            LIMIT 200
                            """,
                            (bot.key,),
                        )
                        rows = list(await cur.fetchall() or [])
                        for row in rows:
                            age = int(row.get("metrics_age_seconds") or 0)
                            stale = age > 45
                            item = {
                                "guild_id": str(row.get("guild_id")),
                                "voice_connected": bool(row.get("voice_connected")),
                                "connected_channel_id": str(row.get("connected_channel_id") or row.get("voice_state_connected_channel_id") or ""),
                                "last_channel_id": str(row.get("last_channel_id") or ""),
                                "desired_connected": bool(row.get("desired_connected")),
                                "player_connected": bool(row.get("player_connected")),
                                "player_playing": bool(row.get("player_playing")),
                                "player_paused": bool(row.get("player_paused")),
                                "queue_count": int(row.get("queue_count") or 0),
                                "backup_queue_count": int(row.get("backup_queue_count") or 0),
                                "is_playing_db": bool(row.get("is_playing_db")),
                                "is_paused_db": bool(row.get("is_paused_db")),
                                "position_seconds": int(row.get("position_seconds") or 0),
                                "recovery_pending": bool(row.get("recovery_pending")),
                                "lavalink_ready": bool(row.get("lavalink_ready")),
                                "reconnect_attempts": int(row.get("reconnect_attempts") or 0),
                                "metrics_age_seconds": age,
                                "voice_age_seconds": int(row.get("voice_age_seconds") or 0),
                                "stale": stale,
                                "last_error": row.get("last_error") or row.get("voice_last_error"),
                            }
                            bot_metrics.append(item)
                            totals["guilds"] += 1
                            totals["voice_connected"] += int(item["voice_connected"])
                            totals["playing"] += int(item["player_playing"])
                            totals["paused"] += int(item["player_paused"])
                            totals["queued_tracks"] += item["queue_count"]
                            totals["backup_tracks"] += item["backup_queue_count"]
                            totals["recovering"] += int(item["recovery_pending"])
                            totals["lavalink_ready"] += int(item["lavalink_ready"])
                            totals["stale_metrics"] += int(stale)
                    except Exception as exc:
                        msg = str(exc)
                        if "doesn't exist" in msg or "1146" in msg:
                            error = None
                        else:
                            error = msg

                    totals["bots"] += 1
                    bots.append({
                        "key": bot.key,
                        "display_name": bot.display_name,
                        "schema": schema,
                        "metrics": bot_metrics,
                        "error": error,
                        "status": "error" if error else ("stale" if any(m["stale"] for m in bot_metrics) else "ok"),
                    })

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "totals": totals,
            "bots": bots,
        }


    async def get_recent_aria_medic_events(self, limit: int = 25) -> list[dict[str, Any]]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        bounded_limit = max(1, min(int(limit), 50))
        events: list[dict[str, Any]] = []
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(
                        "SELECT event_type, bot_name, guild_id, severity, created_at FROM `discord_aria`.`aria_swarm_events` ORDER BY created_at DESC, id DESC LIMIT %s",
                        (bounded_limit,),
                    )
                    for row in list(await cur.fetchall() or []):
                        event_type = str(row.get("event_type") or "aria_event")
                        bot_name = str(row.get("bot_name") or "aria")
                        guild_id = row.get("guild_id")
                        severity = str(row.get("severity") or "info").lower()
                        level = "error" if severity in {"critical", "error"} else ("warning" if severity in {"warning", "recoverable", "degraded"} else "info")
                        desc = event_type.replace("_", " ")
                        if bot_name:
                            desc += f" | bot={bot_name}"
                        if guild_id not in (None, "", 0, "0"):
                            desc += f" | guild={guild_id}"
                        events.append({
                            "type": "aria_medic_event",
                            "level": level,
                            "title": "Aria Medic Event",
                            "description": desc,
                            "source": "aria",
                            "timestamp": row.get("created_at").isoformat() if row.get("created_at") else datetime.now(timezone.utc).isoformat(),
                        })
                except Exception:
                    pass
                try:
                    await cur.execute(
                        "SELECT target_name, action_name, issue_type, success, execution_mode, created_at FROM `discord_aria`.`aria_infra_history` ORDER BY created_at DESC, id DESC LIMIT %s",
                        (max(3, min(10, bounded_limit // 2 + 1)),),
                    )
                    for row in list(await cur.fetchall() or []):
                        level = "info" if int(row.get("success") or 0) else ("warning" if str(row.get("execution_mode") or "") == "planned" else "error")
                        events.append({
                            "type": "aria_infra_event",
                            "level": level,
                            "title": "Aria Infra Action",
                            "description": f"{row.get('action_name') or 'action'} -> {row.get('target_name') or 'target'} | {row.get('issue_type') or 'issue'} | mode={row.get('execution_mode') or 'unknown'}",
                            "source": "aria",
                            "timestamp": row.get("created_at").isoformat() if row.get("created_at") else datetime.now(timezone.utc).isoformat(),
                        })
                except Exception:
                    pass
        events.sort(key=lambda item: item.get("timestamp") or "")
        return events[-bounded_limit:]


    async def get_recent_bot_error_events(self, limit: int = 50) -> list[dict[str, Any]]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        per_bot_limit = max(3, min(25, int(limit // max(len(MUSIC_BOTS), 1)) + 2))
        events: list[dict[str, Any]] = []
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                for bot in MUSIC_BOTS:
                    schema = bot.db_schema
                    if not schema or not bot.table_prefix:
                        continue
                    table_name = f"{bot.table_prefix}_error_events"
                    try:
                        await cur.execute(
                            f"""
                            SELECT id, bot_name, guild_id, error_level, error_type, title, description, traceback_text, created_at
                            FROM `{schema}`.`{table_name}`
                            ORDER BY created_at DESC, id DESC
                            LIMIT %s
                            """,
                            (per_bot_limit,),
                        )
                        rows = list(await cur.fetchall() or [])
                    except Exception:
                        continue

                    for row in rows:
                        created_at = row.get("created_at")
                        timestamp = created_at.astimezone(timezone.utc).isoformat() if hasattr(created_at, 'astimezone') else datetime.now(timezone.utc).isoformat()
                        description = (row.get("description") or "").strip()
                        traceback_text = (row.get("traceback_text") or "").strip()
                        if traceback_text:
                            description = (description + "\n\n" + traceback_text).strip()
                        events.append(
                            {
                                "type": "bot_error",
                                "level": (row.get("error_level") or "error").lower(),
                                "title": row.get("title") or f"{bot.display_name} Error",
                                "description": description,
                                "source": bot.key,
                                "timestamp": timestamp,
                                "bot_key": bot.key,
                                "guild_id": str(row.get("guild_id")) if row.get("guild_id") is not None else None,
                                "error_type": row.get("error_type") or "runtime",
                            }
                        )

        events.sort(key=lambda item: item.get("timestamp") or "")
        return events[-max(1, min(int(limit), 100)):]

    async def truncate_table(self, schema: str, table: str) -> None:
        schema = _validate_identifier(schema, "schema")
        table = _validate_identifier(table, "table")
        if schema in SYSTEM_SCHEMAS:
            raise ValueError(f"Refusing operation on system schema: {schema}")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                await cur.execute(f"TRUNCATE TABLE `{schema}`.`{table}`")
                await cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    async def truncate_schema(self, schema: str) -> dict[str, Any]:
        schema = _validate_identifier(schema, "schema")
        if schema in SYSTEM_SCHEMAS:
            raise ValueError(f"Refusing operation on system schema: {schema}")
        tables = await self.list_tables(schema)
        for table in tables:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                    await cur.execute(f"TRUNCATE TABLE `{schema}`.`{table['table_name']}`")
                    await cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        return {"schema": schema, "truncated_tables": len(tables), "tables": [t["table_name"] for t in tables]}

    async def get_table_data(self, schema: str, table: str, limit: int = 100) -> dict[str, Any]:
        schema = _validate_identifier(schema, "schema")
        table = _validate_identifier(table, "table")
        if schema in SYSTEM_SCHEMAS:
            raise ValueError(f"Refusing operation on system schema: {schema}")
            
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        async with self.pool.acquire() as conn:
            # Use DictCursor so the frontend gets column names alongside the values
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(f"SELECT * FROM `{schema}`.`{table}` LIMIT %s", (limit,))
                rows = await cur.fetchall()
                
                # Sanitize the data for JSON serialization
                processed_rows = []
                for row in rows:
                    processed_row = {}
                    for key, val in row.items():
                        if isinstance(val, datetime):
                            processed_row[key] = val.isoformat()
                        elif isinstance(val, bytes):
                            processed_row[key] = "<binary data>"
                        else:
                            processed_row[key] = val
                    processed_rows.append(processed_row)

                return {
                    "schema": schema,
                    "table": table,
                    "count": len(processed_rows),
                    "rows": processed_rows
                }

    async def control_bot(self, bot_key: str, guild_id: str, action: str, payload: Any = None) -> dict[str, Any]:
        bot = BOT_INDEX.get(bot_key)
        if not bot:
            raise ValueError("Invalid bot")
        if bot.kind != "music" or not bot.db_schema or not bot.table_prefix:
            raise ValueError("This action is only supported for music bots")

        schema = _validate_identifier(bot.db_schema, "schema")
        prefix = _validate_identifier(bot.table_prefix, "table prefix")
        gid = _coerce_int(guild_id, "guild_id")
        action = str(action or "").strip().upper()

        result: dict[str, Any] = {"action": action, "command": action}

        async with self.pool.acquire() as conn:
            # Explicit DictCursor fixes Shuffle crashes, explicit commit fixes silent rollbacks
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if action in ["PAUSE", "RESUME", "SKIP", "STOP"]:
                    await asyncio.wait_for(cur.execute(f"CREATE TABLE IF NOT EXISTS `{_validate_identifier(schema, 'schema')}`.`{_validate_identifier(prefix, 'table prefix')}_swarm_overrides` (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"), timeout=15)
                    await cur.execute(f"REPLACE INTO `{schema}`.`{prefix}_swarm_overrides` (guild_id, bot_name, command) VALUES (%s, %s, %s)", (gid, bot_key, action))
                    # Mirror the intended pause/resume state immediately so the panel does not lag behind Discord.
                    try:
                        await cur.execute(f"ALTER TABLE `{schema}`.`{prefix}_playback_state` ADD COLUMN is_paused BOOLEAN DEFAULT FALSE")
                    except Exception:
                        pass
                    if action == "PAUSE":
                        await cur.execute(
                            f"UPDATE `{schema}`.`{prefix}_playback_state` SET is_paused = TRUE, is_playing = FALSE WHERE guild_id = %s AND bot_name = %s",
                            (gid, bot_key),
                        )
                    elif action == "RESUME":
                        await cur.execute(
                            f"UPDATE `{schema}`.`{prefix}_playback_state` SET is_paused = FALSE, is_playing = TRUE WHERE guild_id = %s AND bot_name = %s",
                            (gid, bot_key),
                        )
                    elif action in {"STOP", "SKIP"}:
                        await cur.execute(
                            f"UPDATE `{schema}`.`{prefix}_playback_state` SET is_paused = FALSE WHERE guild_id = %s AND bot_name = %s",
                            (gid, bot_key),
                        )
                    result["message"] = f"{bot.display_name} will {action.lower()} in guild {gid}."
                
                elif action == "RESTART":
                    # BUG FIX: bots poll swarm_overrides every 2 s, but they NEVER read
                    # swarm_health for a RESTART signal.  Writing to swarm_health was a
                    # silent no-op.  Corrected: write RESTART to swarm_overrides (guild 0)
                    # so aria_command_listener picks it up and calls sys.exit(0).
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_overrides` "
                        "(guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), "
                        "PRIMARY KEY(guild_id, bot_name))"
                    )
                    await cur.execute(
                        f"REPLACE INTO `{schema}`.`{prefix}_swarm_overrides` "
                        "(guild_id, bot_name, command) VALUES (%s, %s, %s)",
                        (0, bot_key, "RESTART"),
                    )
                    # Mark only this bot's runtime flags stale without wiping recovery metadata for other bots.
                    try:
                        await cur.execute(
                            f"UPDATE `{schema}`.`{prefix}_playback_state` SET is_playing = FALSE, is_paused = FALSE WHERE bot_name = %s",
                            (bot_key,),
                        )
                    except Exception:
                        pass
                    result["message"] = f"Restart signal queued for {bot.display_name}."

                elif action == "CLEAR":
                    await asyncio.wait_for(cur.execute(f"CREATE TABLE IF NOT EXISTS `{_validate_identifier(schema, 'schema')}`.`{_validate_identifier(prefix, 'table prefix')}_swarm_overrides` (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"), timeout=15)
                    await cur.execute(
                        f"REPLACE INTO `{schema}`.`{prefix}_swarm_overrides` (guild_id, bot_name, command) VALUES (%s, %s, %s)",
                        (gid, bot_key, "STOP"),
                    )
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_queue` "
                        "(id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, "
                        "bot_name VARCHAR(50), video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL)"
                    )
                    # Scope every clear by bot_name; clear backup + voice desired state so cleared tracks do not resurrect.
                    await cur.execute(f"DELETE FROM `{schema}`.`{prefix}_queue` WHERE guild_id = %s AND bot_name = %s", (gid, bot_key))
                    try:
                        await cur.execute(f"DELETE FROM `{schema}`.`{prefix}_queue_backup` WHERE guild_id = %s AND bot_name = %s", (gid, bot_key))
                    except Exception:
                        pass
                    try:
                        await cur.execute(
                            f"UPDATE `{schema}`.`{prefix}_playback_state` "
                            "SET title = NULL, video_url = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE "
                            "WHERE guild_id = %s AND bot_name = %s",
                            (gid, bot_key),
                        )
                    except Exception:
                        try:
                            await cur.execute(
                                f"UPDATE `{schema}`.`{prefix}_playback_state` "
                                "SET title = NULL, position_seconds = 0, is_playing = FALSE, is_paused = FALSE "
                                "WHERE guild_id = %s AND bot_name = %s",
                                (gid, bot_key),
                            )
                        except Exception:
                            pass
                    try:
                        await cur.execute(
                            f"UPDATE `{schema}`.`{prefix}_voice_state` SET desired_connected = FALSE, connected_channel_id = NULL, disconnected_at = CURRENT_TIMESTAMP "
                            "WHERE guild_id = %s AND bot_name = %s",
                            (gid, bot_key),
                        )
                    except Exception:
                        pass
                    result["message"] = f"Cleared the queue and current playback for guild {gid} on {bot.display_name}."

                elif action == "LOOP":
                    mode = _normalize_loop_mode(payload)
                    await self._ensure_music_guild_settings_schema(cur, schema, prefix)
                    await cur.execute(
                        f"INSERT INTO `{schema}`.`{prefix}_guild_settings` (guild_id, loop_mode) VALUES (%s, %s) "
                        f"ON DUPLICATE KEY UPDATE loop_mode = VALUES(loop_mode)",
                        (gid, mode),
                    )
                    result["loop_mode"] = mode
                    result["message"] = f"Loop mode set to {mode} for guild {gid} on {bot.display_name}."

                elif action == "FILTER":
                    mode = _normalize_filter_mode(payload)
                    await self._ensure_music_guild_settings_schema(cur, schema, prefix)
                    await cur.execute(
                        f"INSERT INTO `{schema}`.`{prefix}_guild_settings` (guild_id, filter_mode) VALUES (%s, %s) "
                        f"ON DUPLICATE KEY UPDATE filter_mode = VALUES(filter_mode)",
                        (gid, mode),
                    )
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_overrides` "
                        "(guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"
                    )
                    await cur.execute(
                        f"REPLACE INTO `{schema}`.`{prefix}_swarm_overrides` (guild_id, bot_name, command) VALUES (%s, %s, %s)",
                        (gid, bot_key, "UPDATE_FILTER"),
                    )
                    result["filter_mode"] = mode
                    result["message"] = f"Filter mode set to {mode} for guild {gid} on {bot.display_name}."

                elif action == "SHUFFLE":
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_queue` "
                        "(id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, "
                        "bot_name VARCHAR(50), video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL)"
                    )
                    await cur.execute(f"SELECT * FROM `{schema}`.`{prefix}_queue` WHERE guild_id = %s AND bot_name = %s ORDER BY id ASC", (gid, bot_key))
                    q = await cur.fetchall()
                    if len(q) > 1:
                        import random
                        l = list(q)
                        first = l.pop(0) # Preserve the currently playing song at the top
                        random.shuffle(l)
                        l.insert(0, first)
                        
                        await cur.execute(f"DELETE FROM `{schema}`.`{prefix}_queue` WHERE guild_id = %s AND bot_name = %s", (gid, bot_key))
                        
                        cols = [k for k in l[0].keys() if k != 'id']
                        col_names = ", ".join(f"`{c}`" for c in cols)
                        placeholders = ", ".join("%s" for _ in cols)
                        
                        for row in l:
                            values = tuple(row[c] for c in cols)
                            await cur.execute(f"INSERT INTO `{schema}`.`{prefix}_queue` ({col_names}) VALUES ({placeholders})", values)
                    result["message"] = f"Shuffled the queue for guild {gid} on {bot.display_name}."

                elif action == "PLAY":
                    if not isinstance(payload, dict):
                        raise ValueError("PLAY payload must be an object with source_url and voice_channel_id")

                    source_url = str(payload.get("source_url") or payload.get("query") or "").strip()
                    if not source_url:
                        raise ValueError("Missing source_url for PLAY action")

                    voice_channel_id = _coerce_int(payload.get("voice_channel_id"), "voice_channel_id")
                    text_channel_raw = payload.get("text_channel_id")
                    text_channel_id = _coerce_int(text_channel_raw, "text_channel_id") if text_channel_raw not in (None, "", 0, "0") else 0

                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_direct_orders` ("
                        "id INT AUTO_INCREMENT PRIMARY KEY, "
                        "bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, "
                        "command VARCHAR(50), data TEXT)"
                    )
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_overrides` "
                        "(guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"
                    )
                    await cur.execute(
                        f"DELETE FROM `{schema}`.`{prefix}_swarm_overrides` WHERE guild_id = %s AND bot_name = %s",
                        (gid, bot_key),
                    )
                    await cur.execute(
                        f"DELETE FROM `{schema}`.`{prefix}_swarm_direct_orders` WHERE guild_id = %s AND bot_name = %s",
                        (gid, bot_key),
                    )
                    await cur.execute(
                        f"INSERT INTO `{schema}`.`{prefix}_swarm_direct_orders` "
                        "(bot_name, guild_id, vc_id, text_channel_id, command, data) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (bot_key, gid, voice_channel_id, text_channel_id, "PLAY", source_url),
                    )
                    result["message"] = f"Queued a direct PLAY order for {bot.display_name} in guild {gid}."

                elif action == "LEAVE":
                    force_leave = False
                    if isinstance(payload, dict):
                        force_leave = bool(payload.get("force"))
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_direct_orders` ("
                        "id INT AUTO_INCREMENT PRIMARY KEY, "
                        "bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, "
                        "command VARCHAR(50), data TEXT)"
                    )
                    await cur.execute(
                        f"DELETE FROM `{schema}`.`{prefix}_swarm_direct_orders` WHERE guild_id = %s AND bot_name = %s",
                        (gid, bot_key),
                    )
                    await cur.execute(
                        f"INSERT INTO `{schema}`.`{prefix}_swarm_direct_orders` "
                        "(bot_name, guild_id, vc_id, text_channel_id, command, data) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (bot_key, gid, 0, 0, "LEAVE", "force" if force_leave else ""),
                    )
                    result["message"] = f"Queued a direct LEAVE order for {bot.display_name} in guild {gid}."

                elif action == "SET_HOME":
                    if not isinstance(payload, dict):
                        raise ValueError("SET_HOME payload must be an object with voice_channel_id")

                    voice_channel_id = _coerce_int(payload.get("voice_channel_id"), "voice_channel_id")
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_bot_home_channels` "
                        "(guild_id BIGINT, bot_name VARCHAR(50), home_vc_id BIGINT, PRIMARY KEY (guild_id, bot_name))"
                    )
                    await cur.execute(
                        f"REPLACE INTO `{schema}`.`{prefix}_bot_home_channels` (guild_id, bot_name, home_vc_id) VALUES (%s, %s, %s)",
                        (gid, bot_key, voice_channel_id),
                    )
                    result["voice_channel_id"] = voice_channel_id
                    result["message"] = f"Set home channel for {bot.display_name} in guild {gid}."

                else:
                    raise ValueError(f"Unsupported action: {action}")
            
            await conn.commit() # FORCE COMMIT TO DATABASE
        return result
