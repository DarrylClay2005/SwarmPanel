import asyncio
import base64
import hashlib
import json
import logging
import re
import secrets
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
ACCOUNT_USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{2,80}$")
ACCOUNT_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
ACCOUNT_LOGIN_SCHEMA = "accountlogins"
ACCOUNT_LOGIN_TABLE = "users"
ACCOUNT_GUILD_LOCK_TABLE = "guild_locks"
ACCOUNT_PROFILE_COLUMNS = (
    ("email", "VARCHAR(255) NULL"),
    ("email_verified_at", "TIMESTAMP NULL DEFAULT NULL"),
    ("email_verification_token_hash", "CHAR(64) NULL"),
    ("email_verification_sent_at", "TIMESTAMP NULL DEFAULT NULL"),
    ("display_name", "VARCHAR(80) NULL"),
    ("avatar_url", "TEXT NULL"),
    ("bio", "VARCHAR(280) NULL"),
    ("favorite_bot", "VARCHAR(50) NULL"),
    ("theme_accent", "VARCHAR(20) NULL"),
    ("public_profile", "TINYINT(1) NOT NULL DEFAULT 1"),
    ("server_invite_url", "TEXT NULL"),
    ("server_name", "VARCHAR(120) NULL"),
    ("server_icon_url", "TEXT NULL"),
    ("panel_preferences", "JSON NULL"),
    ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
)
ACCOUNT_PROFILE_FIELDS = {name for name, _definition in ACCOUNT_PROFILE_COLUMNS}
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


def _normalize_account_username(value: Any) -> str:
    username = str(value or "").strip()
    if not ACCOUNT_USERNAME_RE.fullmatch(username):
        raise ValueError("Username must be 2-80 characters using letters, numbers, dots, dashes, or underscores.")
    return username


def _normalize_email(value: Any) -> str | None:
    email = str(value or "").strip().lower()
    if not email:
        return None
    if len(email) > 255 or not ACCOUNT_EMAIL_RE.fullmatch(email):
        raise ValueError("Enter a valid email address.")
    return email


def _verification_token_hash(token: str) -> str:
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def _gallery_password_hash(password: str) -> str:
    salt = secrets.token_bytes(16)
    iterations = 260_000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${base64.b64encode(salt).decode()}${base64.b64encode(digest).decode()}"


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


    async def _ensure_schema_exists(self, schema_name: str) -> None:
        schema_name = _validate_identifier(schema_name, "schema")
        conn = await aiomysql.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            user=self.settings.db_user,
            password=self.settings.db_password,
            autocommit=True,
            connect_timeout=10,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"CREATE DATABASE IF NOT EXISTS `{schema_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        finally:
            conn.close()

    async def connect(self) -> None:
        if not self.pool:
            # Create every schema the dashboard can read before the pool opens.
            startup_schemas = {self.settings.db_default_schema, "discord_aria", ACCOUNT_LOGIN_SCHEMA}
            startup_schemas.update(bot.db_schema for bot in MUSIC_BOTS if bot.db_schema)
            for schema in sorted(startup_schemas):
                await self._ensure_schema_exists(schema)
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
        await self._ensure_startup_schema()
        if not self.http_session:
            timeout = aiohttp.ClientTimeout(total=10)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

    async def _ensure_startup_schema(self) -> None:
        """Create low-churn panel/Aria support tables once at startup.

        Dashboard polling must never execute DDL. Running this from connect() keeps
        the hot dashboard path read-only and avoids repeated metadata locks.
        """
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await asyncio.wait_for(cur.execute(
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
                    ), timeout=15)
                except Exception as exc:
                    logger.warning("Could not ensure discord_aria.aria_interactions: %s", exc)
                try:
                    await asyncio.wait_for(cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}` (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            username VARCHAR(80) NOT NULL UNIQUE,
                            guild_id BIGINT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_login_at TIMESTAMP NULL DEFAULT NULL,
                            INDEX idx_accountlogins_guild_id (guild_id)
                        )
                        """
                    ), timeout=15)
                except Exception as exc:
                    logger.warning("Could not ensure accountlogins.users: %s", exc)
                try:
                    await asyncio.wait_for(cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` (
                            guild_id BIGINT NOT NULL PRIMARY KEY,
                            username VARCHAR(80) NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    ), timeout=15)
                    await asyncio.wait_for(cur.execute(
                        f"""
                        INSERT IGNORE INTO `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` (guild_id, username)
                        SELECT guild_id, MIN(username)
                        FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
                        GROUP BY guild_id
                        """
                    ), timeout=15)
                except Exception as exc:
                    logger.warning("Could not ensure accountlogins.guild_locks: %s", exc)
                try:
                    await self._ensure_account_profile_schema(cur)
                except Exception as exc:
                    logger.warning("Could not ensure account profile columns: %s", exc)

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
        if self.http_session:
            await self.http_session.close()
            self.http_session = None

    async def _ensure_connected(self) -> None:
        if not self.pool:
            await self.connect()

    async def _fetchall(self, query: str, params: tuple[Any, ...] = (), dict_cursor: bool = True):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            cursor_cls = aiomysql.DictCursor if dict_cursor else None
            async with conn.cursor(cursor_cls) as cur:
                await asyncio.wait_for(cur.execute(query, params), timeout=15)
                return await asyncio.wait_for(cur.fetchall(), timeout=15)

    async def _fetchone(self, query: str, params: tuple[Any, ...] = (), dict_cursor: bool = True):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            cursor_cls = aiomysql.DictCursor if dict_cursor else None
            async with conn.cursor(cursor_cls) as cur:
                await asyncio.wait_for(cur.execute(query, params), timeout=15)
                return await asyncio.wait_for(cur.fetchone(), timeout=15)

    async def _execute(self, query: str, params: tuple[Any, ...] = ()) -> int:
        await self._ensure_connected()
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

    async def _ensure_account_profile_schema(self, cur) -> None:
        for column_name, definition in ACCOUNT_PROFILE_COLUMNS:
            try:
                safe_column = _validate_identifier(column_name, "account profile column")
                await asyncio.wait_for(cur.execute(
                    f"ALTER TABLE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}` "
                    f"ADD COLUMN `{safe_column}` {definition}"
                ), timeout=15)
            except Exception:
                pass
        for index_name, columns in (
            ("idx_accountlogins_public_username", "`public_profile`, `username`"),
            ("idx_accountlogins_server_name", "`server_name`"),
        ):
            try:
                safe_index = _validate_identifier(index_name, "account profile index")
                await asyncio.wait_for(cur.execute(
                    f"CREATE INDEX `{safe_index}` ON `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}` ({columns})"
                ), timeout=15)
            except Exception:
                pass
        try:
            await asyncio.wait_for(cur.execute(
                f"CREATE UNIQUE INDEX `uniq_accountlogins_email` ON `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}` (`email`)"
            ), timeout=15)
        except Exception:
            pass

    async def register_account_login(
        self,
        username: str,
        guild_id: str | int,
        email: str | None = None,
        email_verification_token: str | None = None,
    ) -> dict[str, Any]:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        email = _normalize_email(email)
        token_hash = _verification_token_hash(email_verification_token) if email and email_verification_token else None
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await asyncio.wait_for(conn.begin(), timeout=15)
                try:
                    await asyncio.wait_for(cur.execute(
                        f"""
                        SELECT username, guild_id, email
                        FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
                        WHERE username = %s OR guild_id = %s OR (%s IS NOT NULL AND email = %s)
                        LIMIT 1
                        """,
                        (username, gid, email, email),
                    ), timeout=15)
                    existing = await asyncio.wait_for(cur.fetchone(), timeout=15)
                    if existing:
                        if existing.get("username") == username:
                            raise ValueError("That username is already registered.")
                        if email and existing.get("email") == email:
                            raise ValueError("That email is already registered.")
                        raise ValueError("That guild ID is already registered to another account.")

                    await asyncio.wait_for(cur.execute(
                        f"""
                        INSERT INTO `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` (guild_id, username)
                        VALUES (%s, %s)
                        """,
                        (gid, username),
                    ), timeout=15)
                    await asyncio.wait_for(cur.execute(
                        f"""
                        INSERT INTO `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}` (
                            username, guild_id, email, email_verification_token_hash, email_verification_sent_at
                        )
                        VALUES (%s, %s, %s, %s, CASE WHEN %s IS NULL THEN NULL ELSE CURRENT_TIMESTAMP END)
                        """,
                        (username, gid, email, token_hash, token_hash),
                    ), timeout=15)
                    await asyncio.wait_for(conn.commit(), timeout=15)
                except ValueError:
                    await asyncio.wait_for(conn.rollback(), timeout=15)
                    raise
                except Exception as exc:
                    await asyncio.wait_for(conn.rollback(), timeout=15)
                    message = str(exc).lower()
                    if "duplicate" in message or "1062" in message:
                        if "guild_locks" in message or str(gid) in message:
                            raise ValueError("That guild ID is already registered to another account.") from exc
                        if email and "email" in message:
                            raise ValueError("That email is already registered.") from exc
                        raise ValueError("That username is already registered.") from exc
                    raise
        return {"username": username, "guild_id": str(gid), "email": email}

    async def verify_account_email_by_token(self, token: str) -> dict[str, Any] | None:
        token_hash = _verification_token_hash(token)
        row = await self._fetchone(
            f"""
            SELECT username, guild_id
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE email_verification_token_hash = %s
            LIMIT 1
            """,
            (token_hash,),
        )
        if not row:
            return None
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET email_verified_at = CURRENT_TIMESTAMP, email_verification_token_hash = NULL
            WHERE username = %s AND guild_id = %s
            """,
            (row["username"], row["guild_id"]),
        )
        return {"username": row["username"], "guild_id": str(row["guild_id"])}

    async def authenticate_account_login(self, username: str, guild_id: str | int) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        row = await self._fetchone(
            f"""
            SELECT username, guild_id, email, email_verified_at
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE username = %s AND guild_id = %s
            LIMIT 1
            """,
            (username, gid),
        )
        if not row:
            return None
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET last_login_at = CURRENT_TIMESTAMP
            WHERE username = %s AND guild_id = %s
            """,
            (username, gid),
        )
        return {"username": row["username"], "guild_id": str(row["guild_id"]), "email": row.get("email"), "email_verified": bool(row.get("email_verified_at"))}

    def _serialize_account_profile(self, row: dict[str, Any]) -> dict[str, Any]:
        profile = dict(row)
        if profile.get("guild_id") is not None:
            profile["guild_id"] = str(profile["guild_id"])
        profile["display_name"] = profile.get("display_name") or profile.get("username")
        profile["public_profile"] = bool(profile.get("public_profile"))
        profile["email_verified"] = bool(profile.get("email_verified_at"))
        preferences = profile.get("panel_preferences")
        if isinstance(preferences, str):
            try:
                preferences = json.loads(preferences)
            except Exception:
                preferences = {}
        elif not isinstance(preferences, dict):
            preferences = {}
        profile["panel_preferences"] = preferences
        for key in ("created_at", "last_login_at", "updated_at"):
            value = profile.get(key)
            if hasattr(value, "isoformat"):
                profile[key] = value.isoformat()
        return profile

    async def get_account_profile(self, username: str, guild_id: str | int) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        row = await self._fetchone(
            f"""
            SELECT username, guild_id, email, email_verified_at, display_name, avatar_url, bio, favorite_bot, theme_accent,
                   public_profile, server_invite_url, server_name, server_icon_url,
                   panel_preferences, created_at, last_login_at, updated_at
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE username = %s AND guild_id = %s
            LIMIT 1
            """,
            (username, gid),
        )
        return self._serialize_account_profile(row) if row else None

    async def update_account_profile(self, username: str, guild_id: str | int, updates: dict[str, Any]) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        safe_updates = {
            key: value
            for key, value in updates.items()
            if key in ACCOUNT_PROFILE_FIELDS and key != "updated_at"
        }
        if safe_updates:
            assignments = ", ".join(f"`{_validate_identifier(key, 'account profile column')}` = %s" for key in safe_updates)
            await self._execute(
                f"""
                UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
                SET {assignments}
                WHERE username = %s AND guild_id = %s
                """,
                (*safe_updates.values(), username, gid),
            )
        return await self.get_account_profile(username, gid)

    async def update_account_panel_preferences(
        self,
        username: str,
        guild_id: str | int,
        preferences: dict[str, Any],
    ) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET panel_preferences = %s
            WHERE username = %s AND guild_id = %s
            """,
            (json.dumps(preferences, separators=(",", ":")), username, gid),
        )
        return await self.get_account_profile(username, gid)

    async def search_account_profiles(self, query: str = "", limit: int = 24) -> list[dict[str, Any]]:
        normalized_query = str(query or "").strip()
        safe_limit = max(1, min(50, int(limit or 24)))
        like = f"%{normalized_query}%"
        rows = await self._fetchall(
            f"""
            SELECT username, guild_id, email, email_verified_at, display_name, avatar_url, bio, favorite_bot, theme_accent,
                   public_profile, server_invite_url, server_name, server_icon_url,
                   created_at, last_login_at, updated_at
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE public_profile = 1
              AND (
                  %s = ''
                  OR username LIKE %s
                  OR COALESCE(display_name, '') LIKE %s
                  OR COALESCE(server_name, '') LIKE %s
                  OR COALESCE(favorite_bot, '') LIKE %s
              )
            ORDER BY COALESCE(updated_at, created_at) DESC, username ASC
            LIMIT %s
            """,
            (normalized_query, like, like, like, like, safe_limit),
        )
        profiles = [self._serialize_account_profile(row) for row in rows]
        summaries = await self.get_music_activity_summary_for_guilds([profile["guild_id"] for profile in profiles])
        for profile in profiles:
            profile["activity"] = summaries.get(profile["guild_id"], self._empty_music_activity_summary())
        return profiles

    def _empty_music_activity_summary(self) -> dict[str, Any]:
        return {"top_tracks": [], "top_bots": [], "active_sessions": [], "total_plays": 0}

    async def get_music_activity_summary_for_guilds(self, guild_ids: list[str | int]) -> dict[str, dict[str, Any]]:
        normalized_guilds = sorted({_coerce_int(guild_id, "guild_id") for guild_id in guild_ids if guild_id not in (None, "")})
        summaries = {str(guild_id): self._empty_music_activity_summary() for guild_id in normalized_guilds}
        if not normalized_guilds:
            return summaries

        placeholders = ", ".join(["%s"] * len(normalized_guilds))
        per_guild_tracks: dict[int, dict[str, dict[str, Any]]] = {guild_id: {} for guild_id in normalized_guilds}
        per_guild_bots: dict[int, dict[str, int]] = {guild_id: {} for guild_id in normalized_guilds}
        per_guild_active: dict[int, list[dict[str, Any]]] = {guild_id: [] for guild_id in normalized_guilds}

        for bot in MUSIC_BOTS:
            if not bot.db_schema or not bot.table_prefix:
                continue
            schema = _validate_identifier(bot.db_schema, "schema")
            prefix = _validate_identifier(bot.table_prefix, "table prefix")
            history_table = f"{prefix}_history"
            playback_table = f"{prefix}_playback_state"

            if await self._table_exists(schema, history_table):
                try:
                    rows = await self._fetchall(
                        f"""
                        SELECT guild_id, title, video_url, COUNT(*) AS plays, MAX(played_at) AS last_played_at
                        FROM `{schema}`.`{history_table}`
                        WHERE guild_id IN ({placeholders}) AND title IS NOT NULL AND title != ''
                        GROUP BY guild_id, title, video_url
                        ORDER BY plays DESC, last_played_at DESC
                        LIMIT 300
                        """,
                        tuple(normalized_guilds),
                    )
                    for row in rows:
                        guild_id = int(row["guild_id"])
                        title = str(row.get("title") or "Unknown Track").strip()
                        play_count = int(row.get("plays") or 0)
                        key = title.lower()
                        existing = per_guild_tracks[guild_id].setdefault(key, {
                            "title": title,
                            "video_url": row.get("video_url"),
                            "plays": 0,
                        })
                        existing["plays"] += play_count
                        per_guild_bots[guild_id][bot.key] = per_guild_bots[guild_id].get(bot.key, 0) + play_count
                    for guild_id in normalized_guilds:
                        summaries[str(guild_id)]["total_plays"] += per_guild_bots[guild_id].get(bot.key, 0)
                except Exception as exc:
                    logger.debug("Could not read %s.%s for user directory: %s", schema, history_table, exc)

            if await self._table_exists(schema, playback_table):
                try:
                    rows = await self._fetchall(
                        f"""
                        SELECT guild_id, title, video_url, is_playing, is_paused
                        FROM `{schema}`.`{playback_table}`
                        WHERE guild_id IN ({placeholders}) AND (is_playing = 1 OR is_paused = 1)
                        """,
                        tuple(normalized_guilds),
                    )
                except Exception:
                    try:
                        rows = await self._fetchall(
                            f"""
                            SELECT guild_id, title, video_url, is_playing
                            FROM `{schema}`.`{playback_table}`
                            WHERE guild_id IN ({placeholders}) AND is_playing = 1
                            """,
                            tuple(normalized_guilds),
                        )
                    except Exception as exc:
                        logger.debug("Could not read %s.%s active state for user directory: %s", schema, playback_table, exc)
                        rows = []
                for row in rows:
                    guild_id = int(row["guild_id"])
                    per_guild_active[guild_id].append({
                        "bot_key": bot.key,
                        "bot_display": bot.display_name,
                        "title": row.get("title") or "Unknown Track",
                        "video_url": row.get("video_url"),
                        "is_playing": bool(row.get("is_playing")),
                        "is_paused": bool(row.get("is_paused")),
                    })

        for guild_id in normalized_guilds:
            tracks = sorted(per_guild_tracks[guild_id].values(), key=lambda item: (-int(item["plays"]), item["title"].lower()))
            bots = sorted(per_guild_bots[guild_id].items(), key=lambda item: (-item[1], item[0]))
            summaries[str(guild_id)]["top_tracks"] = tracks[:3]
            summaries[str(guild_id)]["top_bots"] = [
                {
                    "bot_key": bot_key,
                    "bot_display": BOT_INDEX.get(bot_key).display_name if BOT_INDEX.get(bot_key) else bot_key,
                    "plays": plays,
                }
                for bot_key, plays in bots[:3]
            ]
            summaries[str(guild_id)]["active_sessions"] = per_guild_active[guild_id][:4]

        return summaries

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
        metrics_table = f"{prefix}_metrics"

        table_exists = {
            "playback": await self._table_exists(schema, playback_table),
            "settings": await self._table_exists(schema, settings_table),
            "queue": await self._table_exists(schema, queue_table),
            "backup": await self._table_exists(schema, backup_table),
            "home": await self._table_exists(schema, home_table),
            "direct_orders": await self._table_exists(schema, direct_orders_table),
            "heartbeat": await self._table_exists(schema, heartbeat_table),
            "metrics": await self._table_exists(schema, metrics_table),
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
        metrics_table = f"{prefix}_metrics"

        table_exists = {
            "playback": await self._table_exists(schema, playback_table),
            "settings": await self._table_exists(schema, settings_table),
            "queue": await self._table_exists(schema, queue_table),
            "backup": await self._table_exists(schema, backup_table),
            "home": await self._table_exists(schema, home_table),
            "heartbeat": await self._table_exists(schema, heartbeat_table),
            "metrics": await self._table_exists(schema, metrics_table),
        }

        playback_rows: list[dict[str, Any]] = []
        filter_map: dict[int, dict[str, Any]] = {}
        queue_map: dict[int, int] = {}
        backup_queue_map: dict[int, int] = {}
        home_map: dict[int, int | None] = {}
        metrics_map: dict[int, dict[str, Any]] = {}
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

        if table_exists.get("metrics"):
            try:
                rows = await self._fetchall(
                    f"SELECT *, TIMESTAMPDIFF(SECOND, updated_at, NOW()) AS metric_age_seconds FROM `{schema}`.`{metrics_table}` WHERE bot_name = %s",
                    (bot.key,),
                )
                for row in rows:
                    guild_id = int(row["guild_id"])
                    metrics_map[guild_id] = row
                    known_guilds.add(guild_id)
            except Exception:
                logger.exception("Failed reading metrics for %s", bot.key)

        playback_map = {int(row["guild_id"]): row for row in playback_rows if row.get("guild_id") is not None}
        sessions = []
        active_playing_count = 0
        sorted_guild_ids = sorted(known_guilds)

        for guild_id in sorted_guild_ids:
            playback = playback_map.get(guild_id, {})
            metric = metrics_map.get(guild_id, {})
            settings = filter_map.get(guild_id, {})
            queue_count = queue_map.get(guild_id, 0)
            backup_queue_count = backup_queue_map.get(guild_id, 0)
            home_channel_id = home_map.get(guild_id)
            source_info = _detect_media_source(playback.get("video_url"))
            metric_fresh = int(metric.get("metric_age_seconds") or 999999) <= 90 if metric else False
            is_playing = bool(metric.get("player_playing")) if metric_fresh else bool(playback.get("is_playing"))
            is_paused = bool(metric.get("player_paused")) if metric_fresh else bool(playback.get("is_paused"))
            effective_channel_id = metric.get("connected_channel_id") if metric_fresh and metric.get("connected_channel_id") else playback.get("channel_id")
            effective_position = int(metric.get("position_seconds") or playback.get("position_seconds") or 0)
            effective_playback = {**playback, "is_playing": is_playing, "is_paused": is_paused, "channel_id": effective_channel_id}
            session_state, session_state_label = _derive_session_state(
                effective_playback,
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
                    "channel_id": str(effective_channel_id) if effective_channel_id else None,
                    "title": playback.get("title"),
                    "video_url": playback.get("video_url"),
                    "media_source": source_info["key"],
                    "media_source_label": source_info["label"],
                    "thumbnail": None,
                    "position_seconds": effective_position,
                    "is_playing": is_playing,
                    "is_paused": is_paused,
                    "metric_age_seconds": int(metric.get("metric_age_seconds") or -1) if metric else None,
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
        await self._ensure_connected()
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
        await self._ensure_connected()

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
        await self._ensure_connected()

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
        await self._ensure_connected()

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
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                try:
                    await cur.execute(f"TRUNCATE TABLE `{schema}`.`{table}`")
                finally:
                    await cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    async def truncate_schema(self, schema: str) -> dict[str, Any]:
        schema = _validate_identifier(schema, "schema")
        if schema in SYSTEM_SCHEMAS:
            raise ValueError(f"Refusing operation on system schema: {schema}")
        await self._ensure_connected()
        tables = await self.list_tables(schema)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                try:
                    for table in tables:
                        table_name = _validate_identifier(table["table_name"], "table")
                        await cur.execute(f"TRUNCATE TABLE `{schema}`.`{table_name}`")
                finally:
                    await cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        return {"schema": schema, "truncated_tables": len(tables), "tables": [t["table_name"] for t in tables]}

    async def get_table_data(self, schema: str, table: str, limit: int = 100) -> dict[str, Any]:
        schema = _validate_identifier(schema, "schema")
        table = _validate_identifier(table, "table")
        if schema in SYSTEM_SCHEMAS:
            raise ValueError(f"Refusing operation on system schema: {schema}")

        await self._ensure_connected()

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

    def _json_value(self, value: Any) -> Any:
        if hasattr(value, "isoformat"):
            return value.isoformat()
        if isinstance(value, bytes):
            return "<binary data>"
        return value

    def _json_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {key: self._json_value(value) for key, value in row.items()}

    async def get_image_gallery_admin_data(self, limit: int = 50) -> dict[str, Any]:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        safe_limit = max(1, min(int(limit or 50), 200))
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    f"""
                    SELECT u.id, u.username, u.display_name, u.email, u.email_verified_at,
                           u.created_at, u.last_login_at,
                           COUNT(DISTINCT m.id) AS media_count,
                           COUNT(DISTINCT c.id) AS comment_count
                    FROM `{schema}`.`users` u
                    LEFT JOIN `{schema}`.`media_items` m ON m.user_id = u.id
                    LEFT JOIN `{schema}`.`media_comments` c ON c.user_id = u.id
                    GROUP BY u.id
                    ORDER BY u.created_at DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                users = [self._json_row(row) for row in await cur.fetchall()]

                await cur.execute(
                    f"""
                    SELECT c.id, c.media_id, c.user_id, c.body, c.created_at,
                           u.username, u.display_name,
                           m.title AS media_title
                    FROM `{schema}`.`media_comments` c
                    JOIN `{schema}`.`users` u ON u.id = c.user_id
                    JOIN `{schema}`.`media_items` m ON m.id = c.media_id
                    ORDER BY c.created_at DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                comments = [self._json_row(row) for row in await cur.fetchall()]

                await cur.execute(
                    f"""
                    SELECT m.id, m.user_id, m.title, m.media_kind, m.file_size, m.views, m.downloads,
                           m.created_at, u.username
                    FROM `{schema}`.`media_items` m
                    JOIN `{schema}`.`users` u ON u.id = m.user_id
                    ORDER BY m.created_at DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                media = [self._json_row(row) for row in await cur.fetchall()]
        return {"schema": schema, "users": users, "comments": comments, "media": media}

    async def delete_image_gallery_user(self, user_id: int) -> None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        await self._execute(f"DELETE FROM `{schema}`.`users` WHERE id = %s", (_coerce_int(user_id, "user_id"),))

    async def delete_image_gallery_comment(self, comment_id: int) -> None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        await self._execute(f"DELETE FROM `{schema}`.`media_comments` WHERE id = %s", (_coerce_int(comment_id, "comment_id"),))

    async def reset_image_gallery_user_password(self, user_id: int, new_password: str) -> None:
        if len(str(new_password or "")) < 8:
            raise ValueError("Password must be at least 8 characters.")
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        password_hash = _gallery_password_hash(new_password)
        await self._execute(
            f"UPDATE `{schema}`.`users` SET password_hash = %s WHERE id = %s",
            (password_hash, _coerce_int(user_id, "user_id")),
        )

    async def _clear_pending_orders(self, cur: aiomysql.DictCursor, schema: str, prefix: str, gid: int, bot_key: str) -> None:
        overrides_table = f"{prefix}_swarm_overrides"
        direct_orders_table = f"{prefix}_swarm_direct_orders"
        try:
            await cur.execute(
                f"DELETE FROM `{schema}`.`{overrides_table}` WHERE guild_id = %s AND bot_name = %s",
                (gid, bot_key),
            )
        except Exception:
            pass
        try:
            await cur.execute(
                f"DELETE FROM `{schema}`.`{direct_orders_table}` WHERE guild_id = %s AND bot_name = %s",
                (gid, bot_key),
            )
        except Exception:
            pass

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

        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            # Explicit DictCursor fixes Shuffle crashes, explicit commit fixes silent rollbacks
            async with conn.cursor(aiomysql.DictCursor) as cur:
                if action in ["PAUSE", "RESUME", "SKIP", "STOP"]:
                    await self._clear_pending_orders(cur, schema, prefix, gid, bot_key)
                    await asyncio.wait_for(cur.execute(f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_overrides` (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"), timeout=15)
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
                    await self._clear_pending_orders(cur, schema, prefix, 0, bot_key)
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
                    await self._clear_pending_orders(cur, schema, prefix, gid, bot_key)
                    await asyncio.wait_for(cur.execute(f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_overrides` (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"), timeout=15)
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
                    await self._clear_pending_orders(cur, schema, prefix, gid, bot_key)
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
                        "command VARCHAR(50), data TEXT, attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL)"
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
                        f"DELETE FROM `{schema}`.`{prefix}_swarm_direct_orders` WHERE guild_id = %s AND bot_name = %s AND command = %s",
                        (gid, bot_key, action),
                    )
                    await cur.execute(
                        f"INSERT INTO `{schema}`.`{prefix}_swarm_direct_orders` "
                        "(bot_name, guild_id, vc_id, text_channel_id, command, data) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (bot_key, gid, voice_channel_id, text_channel_id, "PLAY", source_url),
                    )
                    result["message"] = f"Queued a direct PLAY order for {bot.display_name} in guild {gid}."

                elif action == "RECOVER":
                    recover_voice_channel_id = 0
                    if isinstance(payload, dict):
                        raw_recover_vc = payload.get("voice_channel_id") or payload.get("vc_id")
                        recover_voice_channel_id = _coerce_int(raw_recover_vc, "voice_channel_id") if raw_recover_vc not in (None, "", 0, "0") else 0
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_direct_orders` ("
                        "id INT AUTO_INCREMENT PRIMARY KEY, "
                        "bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, "
                        "command VARCHAR(50), data TEXT, attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL)"
                    )
                    await cur.execute(
                        f"INSERT INTO `{schema}`.`{prefix}_swarm_direct_orders` "
                        "(bot_name, guild_id, vc_id, text_channel_id, command, data) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (bot_key, gid, recover_voice_channel_id, 0, "RECOVER", "panel"),
                    )
                    result["message"] = f"Queued a direct RECOVER order for {bot.display_name} in guild {gid}."

                elif action == "LEAVE":
                    await self._clear_pending_orders(cur, schema, prefix, gid, bot_key)
                    force_leave = False
                    if isinstance(payload, dict):
                        force_leave = bool(payload.get("force"))
                    await cur.execute(
                        f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_swarm_direct_orders` ("
                        "id INT AUTO_INCREMENT PRIMARY KEY, "
                        "bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, "
                        "command VARCHAR(50), data TEXT, attempts INT NOT NULL DEFAULT 0, last_error TEXT NULL)"
                    )
                    await cur.execute(
                        f"DELETE FROM `{schema}`.`{prefix}_swarm_direct_orders` WHERE guild_id = %s AND bot_name = %s AND command = %s",
                        (gid, bot_key, action),
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
