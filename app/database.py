import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

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



def _derive_thumbnail_url(video_url: str | None) -> str | None:
    video_id = _extract_youtube_video_id(video_url)
    if not video_id:
        return None
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


class PanelDatabase:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: aiomysql.Pool | None = None

    async def connect(self) -> None:
        if self.pool:
            return
        self.pool = await aiomysql.create_pool(
            host=self.settings.db_host,
            port=self.settings.db_port,
            user=self.settings.db_user,
            password=self.settings.db_password,
            db=self.settings.db_default_schema,
            autocommit=True,
            minsize=1,
            maxsize=10,
        )

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def _fetchall(self, query: str, params: tuple[Any, ...] = (), dict_cursor: bool = True):
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        async with self.pool.acquire() as conn:
            cursor_cls = aiomysql.DictCursor if dict_cursor else None
            async with conn.cursor(cursor_cls) as cur:
                await cur.execute(query, params)
                return await cur.fetchall()

    async def _fetchone(self, query: str, params: tuple[Any, ...] = (), dict_cursor: bool = True):
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        async with self.pool.acquire() as conn:
            cursor_cls = aiomysql.DictCursor if dict_cursor else None
            async with conn.cursor(cursor_cls) as cur:
                await cur.execute(query, params)
                return await cur.fetchone()

    async def _execute(self, query: str, params: tuple[Any, ...] = ()) -> int:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                return cur.rowcount

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
        tables = [f"{prefix}_playback_state", f"{prefix}_guild_settings", f"{prefix}_queue"]
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

    async def _music_bot_snapshot(self, bot: BotDefinition) -> dict[str, Any]:
        assert bot.db_schema and bot.table_prefix
        schema = _validate_identifier(bot.db_schema, "schema")
        prefix = _validate_identifier(bot.table_prefix, "table prefix")
        playback_table = f"{prefix}_playback_state"
        settings_table = f"{prefix}_guild_settings"
        queue_table = f"{prefix}_queue"
        heartbeat_table = "swarm_health"

        table_exists = {
            "playback": await self._table_exists(schema, playback_table),
            "settings": await self._table_exists(schema, settings_table),
            "queue": await self._table_exists(schema, queue_table),
            "heartbeat": await self._table_exists(schema, heartbeat_table),
        }

        playback_rows: list[dict[str, Any]] = []
        filter_map: dict[int, dict[str, Any]] = {}
        queue_map: dict[int, int] = {}
        known_guilds: set[int] = set()

        if table_exists["playback"]:
            try:
                playback_rows = await self._fetchall(
                    f"SELECT guild_id, channel_id, title, video_url, position_seconds, is_playing FROM `{schema}`.`{playback_table}` ORDER BY guild_id"
                )
            except Exception:
                playback_rows = await self._fetchall(
                    f"SELECT guild_id, channel_id, title, position_seconds, is_playing FROM `{schema}`.`{playback_table}` ORDER BY guild_id"
                )
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
                    "loop_mode": row.get("loop_mode") or "off",
                    "shuffle_mode": row.get("shuffle_mode") or 0
                }
                known_guilds.add(guild_id)

        if table_exists["queue"]:
            rows = await self._fetchall(
                f"SELECT guild_id, COUNT(*) AS queue_len FROM `{schema}`.`{queue_table}` GROUP BY guild_id"
            )
            for row in rows:
                guild_id = int(row["guild_id"])
                queue_map[guild_id] = int(row.get("queue_len") or 0)
                known_guilds.add(guild_id)

        playback_map = {int(row["guild_id"]): row for row in playback_rows if row.get("guild_id") is not None}
        sessions = []
        active_playing_count = 0

        for guild_id in sorted(known_guilds):
            playback = playback_map.get(guild_id, {})
            settings = filter_map.get(guild_id, {})
            is_playing = bool(playback.get("is_playing"))
            if is_playing:
                active_playing_count += 1
            sessions.append(
                {
                    "guild_id": str(guild_id),
                    "channel_id": playback.get("channel_id"),
                    "title": playback.get("title"),
                    "video_url": playback.get("video_url"),
                    "thumbnail": _derive_thumbnail_url(playback.get("video_url")),
                    "position_seconds": int(playback.get("position_seconds") or 0),
                    "is_playing": is_playing,
                    "filter_mode": settings.get("filter_mode", "none"),
                    "loop_mode": settings.get("loop_mode", "off"),
                    "shuffle_mode": settings.get("shuffle_mode", 0),
                    "queue_count": queue_map.get(guild_id, 0),
                    "guild_name": None,
                    "channel_name": None,
                }
            )

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
        except Exception:
            pass

        aria_status_real = "OFFLINE"
        if aria_heartbeat_age is not None and aria_heartbeat_age < 120:
            aria_status_real = "ONLINE"
        elif aria_heartbeat_age is None and len(bots) > 0:
            aria_status_real = "ONLINE"

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
            }
        )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "bots": bots,
        }

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

        schema = bot.db_schema
        prefix = bot.table_prefix
        gid = _coerce_int(guild_id, "guild_id")
        action = str(action or "").strip().upper()

        result: dict[str, Any] = {"action": action}

        async with self.pool.acquire() as conn:
            # Explicit DictCursor fixes Shuffle crashes, explicit commit fixes silent rollbacks
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if action in ["PAUSE", "RESUME", "SKIP", "STOP"]:
                    await cur.execute(f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_overrides` (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))")
                    await cur.execute(f"REPLACE INTO `{schema}`.`{prefix}_swarm_overrides` (guild_id, bot_name, command) VALUES (%s, %s, %s)", (gid, bot_key, action))
                    result["message"] = f"{bot.display_name} will {action.lower()} in guild {gid}."
                
                elif action == "RESTART":
                    # Update global health table so the node's system manager actually restarts it
                    await cur.execute(f"CREATE TABLE IF NOT EXISTS `{schema}`.swarm_health (bot_name VARCHAR(50) PRIMARY KEY, status VARCHAR(20), last_pulse TIMESTAMP)")
                    await cur.execute(f"UPDATE `{schema}`.swarm_health SET status = 'RESTART' WHERE bot_name = %s", (bot_key,))
                    # Simulate a global playback stall so the watchdog auto-resumes upon booting back up
                    try:
                        await cur.execute(f"UPDATE `{schema}`.`{prefix}_playback_state` SET is_playing = FALSE")
                    except Exception:
                        pass
                    result["message"] = f"Restart requested for {bot.display_name}."

                elif action == "CLEAR":
                    await cur.execute(f"DELETE FROM `{schema}`.`{prefix}_queue` WHERE guild_id = %s", (gid,))
                    result["message"] = f"Cleared the queue for guild {gid} on {bot.display_name}."

                elif action == "LOOP":
                    mode = _normalize_loop_mode(payload)
                    await cur.execute(
                        f"INSERT INTO `{schema}`.`{prefix}_guild_settings` (guild_id, loop_mode) VALUES (%s, %s) "
                        f"ON DUPLICATE KEY UPDATE loop_mode = VALUES(loop_mode)",
                        (gid, mode),
                    )
                    result["loop_mode"] = mode
                    result["message"] = f"Loop mode set to {mode} for guild {gid} on {bot.display_name}."

                elif action == "SHUFFLE":
                    await cur.execute(f"SELECT * FROM `{schema}`.`{prefix}_queue` WHERE guild_id = %s ORDER BY id ASC", (gid,))
                    q = await cur.fetchall()
                    if len(q) > 1:
                        import random
                        l = list(q)
                        first = l.pop(0) # Preserve the currently playing song at the top
                        random.shuffle(l)
                        l.insert(0, first)
                        
                        await cur.execute(f"DELETE FROM `{schema}`.`{prefix}_queue` WHERE guild_id = %s", (gid,))
                        
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
                        f"INSERT INTO `{schema}`.`{prefix}_swarm_direct_orders` "
                        "(bot_name, guild_id, vc_id, text_channel_id, command, data) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (bot_key, gid, voice_channel_id, text_channel_id, "PLAY", source_url),
                    )
                    result["message"] = f"Queued a direct PLAY order for {bot.display_name} in guild {gid}."

                else:
                    raise ValueError(f"Unsupported action: {action}")
            
            await conn.commit() # FORCE COMMIT TO DATABASE
        return result
