import asyncio
import base64
import copy
import hashlib
import json
import logging
import os
import random
import re
import secrets
import time
from collections import OrderedDict
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
ACCOUNT_AUTH_COLUMNS = (
    ("password_hash", "TEXT NULL"),
)
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
SMART_TITLE_NOISE_RE = re.compile(r"\s*[\[(][^\])]*(?:official|lyrics?|audio|video|visualizer|remaster|sped up|slowed)[^\])]*[\])]\s*", re.IGNORECASE)

PANEL_DB_POOL_MIN_SIZE = max(1, int(os.getenv("PANEL_DB_POOL_MIN_SIZE", "1") or "1"))
PANEL_DB_POOL_MAX_SIZE = max(PANEL_DB_POOL_MIN_SIZE, int(os.getenv("PANEL_DB_POOL_MAX_SIZE", "6") or "6"))
PANEL_DB_CONNECT_TIMEOUT_SECONDS = max(3, int(os.getenv("PANEL_DB_CONNECT_TIMEOUT_SECONDS", "10") or "10"))
PANEL_DB_QUERY_TIMEOUT_SECONDS = max(3.0, float(os.getenv("PANEL_DB_QUERY_TIMEOUT_SECONDS", "15") or "15"))
PANEL_DB_POOL_RECYCLE_SECONDS = max(60, int(os.getenv("PANEL_DB_POOL_RECYCLE_SECONDS", "900") or "900"))
PANEL_TABLE_CACHE_TTL_SECONDS = max(10.0, float(os.getenv("PANEL_TABLE_CACHE_TTL_SECONDS", "120") or "120"))
PANEL_DASHBOARD_CACHE_TTL_SECONDS = max(0.5, float(os.getenv("PANEL_DASHBOARD_CACHE_TTL_SECONDS", "2") or "2"))
PANEL_SCHEMA_CACHE_TTL_SECONDS = max(10.0, float(os.getenv("PANEL_SCHEMA_CACHE_TTL_SECONDS", "120") or "120"))
PANEL_TABLE_DATA_CACHE_TTL_SECONDS = max(2.0, float(os.getenv("PANEL_TABLE_DATA_CACHE_TTL_SECONDS", "20") or "20"))
PANEL_TABLE_DATA_CACHE_MAX_ITEMS = max(16, int(os.getenv("PANEL_TABLE_DATA_CACHE_MAX_ITEMS", "64") or "64"))
PANEL_IMAGE_GALLERY_ADMIN_CACHE_TTL_SECONDS = max(2.0, float(os.getenv("PANEL_IMAGE_GALLERY_ADMIN_CACHE_TTL_SECONDS", "10") or "10"))
PANEL_MUSIC_INTELLIGENCE_CACHE_TTL_SECONDS = max(2.0, float(os.getenv("PANEL_MUSIC_INTELLIGENCE_CACHE_TTL_SECONDS", "20") or "20"))

GUILD_SETTINGS_COLUMNS = (
    ("home_vc_id", "BIGINT"),
    ("volume", "INT DEFAULT 100"),
    ("loop_mode", "VARCHAR(10) DEFAULT 'queue'"),
    ("filter_mode", "VARCHAR(20) DEFAULT 'none'"),
    ("dj_role_id", "BIGINT DEFAULT NULL"),
    ("feedback_channel_id", "BIGINT DEFAULT NULL"),
    ("transition_mode", "VARCHAR(10) DEFAULT 'off'"),
    ("fade_seconds", "FLOAT DEFAULT 5.0"),
    ("fade_curve", "VARCHAR(20) DEFAULT 'linear'"),
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


def _password_hash(password: str) -> str:
    salt = secrets.token_bytes(16)
    iterations = 260_000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${base64.b64encode(salt).decode()}${base64.b64encode(digest).decode()}"


def _verify_password_hash(password: str, stored_hash: str | None) -> bool:
    raw_hash = str(stored_hash or "").strip()
    if not raw_hash:
        return False
    try:
        algorithm, iterations_text, salt_b64, digest_b64 = raw_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_text)
        salt = base64.b64decode(salt_b64.encode())
        expected = base64.b64decode(digest_b64.encode())
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return secrets.compare_digest(actual, expected)
    except Exception:
        return False


def _gallery_password_hash(password: str) -> str:
    return _password_hash(password)


def _account_password_hash(password: str) -> str:
    return _password_hash(password)


def _normalize_account_password(value: Any, field_name: str = "Password") -> str:
    password = str(value or "")
    if len(password) < 8:
        raise ValueError(f"{field_name} must be at least 8 characters.")
    return password


def _normalize_loop_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    valid_modes = {"off", "song", "queue"}
    if mode not in valid_modes:
        raise ValueError(f"Invalid loop mode: {value!r}. Expected one of: {', '.join(sorted(valid_modes))}")
    return mode


def _normalize_filter_mode(value: Any) -> str:
    mode = str(value or "").strip().lower().replace(" ", "")
    valid_modes = {
        "none",
        "nightcore",
        "vaporwave",
        "bassboost",
        "8d",
        "karaoke",
        "tremolo",
        "vibrato",
        "lowpass",
        "lofi",
        "electronic",
        "party",
        "radio",
        "cinema",
    }
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


def _smart_query_from_title(title: str | None) -> str:
    cleaned = re.sub(r"https?://\S+", "", str(title or ""))
    cleaned = SMART_TITLE_NOISE_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -|")
    return cleaned[:180] or str(title or "").strip()[:180]


class PanelDatabase:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: aiomysql.Pool | None = None
        self.http_session: aiohttp.ClientSession | None = None
        self.thumbnail_cache: dict[str, tuple[float, str | None]] = {}
        self._connect_lock = asyncio.Lock()
        self._table_exists_cache: dict[tuple[str, str], tuple[float, bool]] = {}
        self._dashboard_cache: tuple[float, dict[str, Any]] | None = None
        self._schema_cache: tuple[float, list[str]] | None = None
        self._tables_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._table_data_cache: OrderedDict[tuple[str, str, int], tuple[float, dict[str, Any]]] = OrderedDict()
        self._image_gallery_admin_cache: dict[int, tuple[float, dict[str, Any]]] = {}
        self._music_intelligence_cache: dict[tuple[str | None, str | None, int], tuple[float, dict[str, Any]]] = {}

    def _invalidate_hot_caches(self) -> None:
        self._dashboard_cache = None
        self._table_exists_cache.clear()
        self._schema_cache = None
        self._tables_cache.clear()
        self._table_data_cache.clear()
        self._image_gallery_admin_cache.clear()
        self._music_intelligence_cache.clear()

    async def _run_with_timeout(self, awaitable, timeout: float | None = None):
        return await asyncio.wait_for(awaitable, timeout=timeout or PANEL_DB_QUERY_TIMEOUT_SECONDS)

    async def _ensure_schema_exists(self, schema_name: str) -> None:
        schema_name = _validate_identifier(schema_name, "schema")
        conn = await aiomysql.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            user=self.settings.db_user,
            password=self.settings.db_password,
            autocommit=True,
            connect_timeout=PANEL_DB_CONNECT_TIMEOUT_SECONDS,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT 1
                    FROM information_schema.schemata
                    WHERE schema_name = %s
                    LIMIT 1
                    """,
                    (schema_name,),
                )
                if not await cur.fetchone():
                    await cur.execute(f"CREATE DATABASE `{schema_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        finally:
            conn.close()

    async def connect(self) -> None:
        async with self._connect_lock:
            if not self.pool or getattr(self.pool, "closed", False):
                # Create core schemas before the pool opens. Music bot schemas are
                # checked best-effort so one new/missing worker DB cannot brick
                # login, session checks, or the public panel shell.
                core_startup_schemas = {self.settings.db_default_schema, "discord_aria", ACCOUNT_LOGIN_SCHEMA}
                for schema in sorted(core_startup_schemas):
                    await self._ensure_schema_exists(schema)

                music_startup_schemas = {bot.db_schema for bot in MUSIC_BOTS if bot.db_schema}
                for schema in sorted(music_startup_schemas - core_startup_schemas):
                    try:
                        await self._ensure_schema_exists(schema)
                    except Exception as exc:
                        logger.warning(
                            "Music bot schema %s is not available during startup; continuing so login stays online: %s",
                            schema,
                            exc,
                        )
                self.pool = await aiomysql.create_pool(
                    host=self.settings.db_host,
                    port=self.settings.db_port,
                    user=self.settings.db_user,
                    password=self.settings.db_password,
                    db=self.settings.db_default_schema,
                    autocommit=True,
                    minsize=PANEL_DB_POOL_MIN_SIZE,
                    maxsize=PANEL_DB_POOL_MAX_SIZE,
                    connect_timeout=PANEL_DB_CONNECT_TIMEOUT_SECONDS,
                    pool_recycle=PANEL_DB_POOL_RECYCLE_SECONDS,
                )
                self._invalidate_hot_caches()
            await self._ensure_startup_schema()
            if not self.http_session or self.http_session.closed:
                timeout = aiohttp.ClientTimeout(total=10, connect=5)
                connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=300)
                self.http_session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def _ensure_startup_schema(self) -> None:
        """Create low-churn panel/Aria support tables once at startup.

        Dashboard polling must never execute DDL. Running this from connect() keeps
        the hot dashboard path read-only and avoids repeated metadata locks.
        """
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                async def table_exists(schema: str, table: str) -> bool:
                    await asyncio.wait_for(cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                        LIMIT 1
                        """,
                        (schema, table),
                    ), timeout=15)
                    return bool(await asyncio.wait_for(cur.fetchone(), timeout=15))

                try:
                    if not await table_exists("discord_aria", "aria_interactions"):
                        await asyncio.wait_for(cur.execute(
                            """
                            CREATE TABLE `discord_aria`.`aria_interactions` (
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
                    if not await table_exists(ACCOUNT_LOGIN_SCHEMA, ACCOUNT_LOGIN_TABLE):
                        await asyncio.wait_for(cur.execute(
                            f"""
                            CREATE TABLE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}` (
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
                    if not await table_exists(ACCOUNT_LOGIN_SCHEMA, ACCOUNT_GUILD_LOCK_TABLE):
                        await asyncio.wait_for(cur.execute(
                            f"""
                            CREATE TABLE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` (
                                guild_id BIGINT NOT NULL PRIMARY KEY,
                                username VARCHAR(80) NOT NULL,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                            """
                        ), timeout=15)
                    await asyncio.wait_for(cur.execute(
                        f"""
                        INSERT IGNORE INTO `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` (guild_id, username)
                        SELECT users.guild_id, users.username
                        FROM (
                            SELECT guild_id, MIN(username) AS username
                            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
                            GROUP BY guild_id
                        ) AS users
                        LEFT JOIN `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` locks
                          ON locks.guild_id = users.guild_id
                        WHERE locks.guild_id IS NULL
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
        if not self.pool or getattr(self.pool, "closed", False):
            await self.connect()

    async def _get_pool(self) -> aiomysql.Pool:
        await self._ensure_connected()
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        return self.pool

    async def _fetchall(self, query: str, params: tuple[Any, ...] = (), dict_cursor: bool = True):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            cursor_cls = aiomysql.DictCursor if dict_cursor else None
            cursor_context = conn.cursor(cursor_cls) if cursor_cls else conn.cursor()
            async with cursor_context as cur:
                await self._run_with_timeout(cur.execute(query, params))
                return await self._run_with_timeout(cur.fetchall())

    async def _fetchone(self, query: str, params: tuple[Any, ...] = (), dict_cursor: bool = True):
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            cursor_cls = aiomysql.DictCursor if dict_cursor else None
            cursor_context = conn.cursor(cursor_cls) if cursor_cls else conn.cursor()
            async with cursor_context as cur:
                await self._run_with_timeout(cur.execute(query, params))
                return await self._run_with_timeout(cur.fetchone())

    async def _execute(self, query: str, params: tuple[Any, ...] = ()) -> int:
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await self._run_with_timeout(cur.execute(query, params))
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

    async def _ensure_music_intelligence_schema(self, cur, schema: str, prefix: str) -> None:
        schema = _validate_identifier(schema, "schema")
        prefix = _validate_identifier(prefix, "table prefix")
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_track_intelligence` ("
            "guild_id BIGINT NOT NULL, url_key VARCHAR(64) NOT NULL, video_url TEXT, title TEXT, "
            "queued_count INT NOT NULL DEFAULT 0, play_count INT NOT NULL DEFAULT 0, finish_count INT NOT NULL DEFAULT 0, "
            "skip_count INT NOT NULL DEFAULT 0, like_count INT NOT NULL DEFAULT 0, dislike_count INT NOT NULL DEFAULT 0, "
            "total_listen_seconds INT NOT NULL DEFAULT 0, last_requester_id BIGINT DEFAULT NULL, source VARCHAR(40) DEFAULT 'unknown', "
            "first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_queued TIMESTAMP NULL DEFAULT NULL, last_played TIMESTAMP NULL DEFAULT NULL, "
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, url_key))"
        )
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_user_track_affinity` ("
            "guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, url_key VARCHAR(64) NOT NULL, video_url TEXT, title TEXT, "
            "queued_count INT NOT NULL DEFAULT 0, play_count INT NOT NULL DEFAULT 0, finish_count INT NOT NULL DEFAULT 0, "
            "skip_count INT NOT NULL DEFAULT 0, like_count INT NOT NULL DEFAULT 0, dislike_count INT NOT NULL DEFAULT 0, "
            "score FLOAT NOT NULL DEFAULT 0, last_requested TIMESTAMP NULL DEFAULT NULL, "
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (guild_id, user_id, url_key))"
        )
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_smart_recommendations` ("
            "id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT NOT NULL, requester_id BIGINT DEFAULT NULL, "
            "seed_title TEXT, seed_url TEXT, query_text TEXT, chosen_url TEXT, chosen_title TEXT, "
            "reason VARCHAR(80), accepted BOOLEAN DEFAULT TRUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        for stmt in (
            f"CREATE INDEX {prefix}_track_intelligence_recent_idx ON `{schema}`.`{prefix}_track_intelligence` (guild_id, last_played)",
            f"CREATE INDEX {prefix}_track_intelligence_requester_idx ON `{schema}`.`{prefix}_track_intelligence` (guild_id, last_requester_id, last_played)",
            f"CREATE INDEX {prefix}_user_affinity_recent_idx ON `{schema}`.`{prefix}_user_track_affinity` (guild_id, user_id, last_requested)",
            f"CREATE INDEX {prefix}_smart_recommendations_recent_idx ON `{schema}`.`{prefix}_smart_recommendations` (guild_id, created_at)",
        ):
            try:
                await cur.execute(stmt)
            except Exception:
                pass

    async def _ensure_account_profile_schema(self, cur) -> None:
        for column_name, definition in (*ACCOUNT_AUTH_COLUMNS, *ACCOUNT_PROFILE_COLUMNS):
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
        password: str,
        email: str | None = None,
        email_verification_token: str | None = None,
    ) -> dict[str, Any]:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        normalized_password = _normalize_account_password(password)
        password_hash = _account_password_hash(normalized_password)
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
                            username, guild_id, password_hash, email, email_verification_token_hash, email_verification_sent_at
                        )
                        VALUES (%s, %s, %s, %s, %s, CASE WHEN %s IS NULL THEN NULL ELSE CURRENT_TIMESTAMP END)
                        """,
                        (username, gid, password_hash, email, token_hash, token_hash),
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

    async def issue_account_email_verification_token(self, username: str, guild_id: str | int, token: str) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        token_hash = _verification_token_hash(token)
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET email_verification_token_hash = %s, email_verification_sent_at = CURRENT_TIMESTAMP
            WHERE username = %s AND guild_id = %s AND email IS NOT NULL AND email_verified_at IS NULL
            """,
            (token_hash, username, gid),
        )
        return await self.get_account_profile(username, gid)

    async def update_account_email(self, username: str, guild_id: str | int, email: str | None) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        normalized = _normalize_email(email)
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET email = %s, email_verified_at = NULL, email_verification_token_hash = NULL, email_verification_sent_at = NULL
            WHERE username = %s AND guild_id = %s
            """,
            (normalized, username, gid),
        )
        return await self.get_account_profile(username, gid)

    async def verify_account_email_code(self, username: str, guild_id: str | int, code: str) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        token_hash = _verification_token_hash(code)
        row = await self._fetchone(
            f"""
            SELECT username, guild_id
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE username = %s AND guild_id = %s AND email IS NOT NULL AND email_verification_token_hash = %s
            LIMIT 1
            """,
            (username, gid, token_hash),
        )
        if not row:
            return None
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET email_verified_at = CURRENT_TIMESTAMP, email_verification_token_hash = NULL
            WHERE username = %s AND guild_id = %s
            """,
            (username, gid),
        )
        return await self.get_account_profile(username, gid)

    async def authenticate_account_login(self, username: str, password: str) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        secret = str(password or "")
        if not secret:
            return None
        row = await self._fetchone(
            f"""
            SELECT username, guild_id, email, email_verified_at, password_hash
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE username = %s
            LIMIT 1
            """,
            (username,),
        )
        if not row:
            return None
        password_hash = row.get("password_hash")
        password_ok = _verify_password_hash(secret, password_hash) if password_hash else False
        legacy_ok = False
        if not password_ok and not password_hash:
            legacy_ok = secrets.compare_digest(str(row.get("guild_id") or ""), secret.strip())
        if not password_ok and not legacy_ok:
            return None
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET last_login_at = CURRENT_TIMESTAMP
            WHERE username = %s AND guild_id = %s
            """,
            (username, row["guild_id"]),
        )
        return {
            "username": row["username"],
            "guild_id": str(row["guild_id"]),
            "email": row.get("email"),
            "email_verified": bool(row.get("email_verified_at")),
            "has_password": bool(password_hash),
            "used_legacy_login": legacy_ok,
        }

    async def get_account_guild_id_for_username(self, username: str) -> str | None:
        username = _normalize_account_username(username)
        row = await self._fetchone(
            f"""
            SELECT guild_id
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE username = %s
            LIMIT 1
            """,
            (username,),
        )
        return str(row["guild_id"]) if row and row.get("guild_id") is not None else None

    def _serialize_account_profile(self, row: dict[str, Any]) -> dict[str, Any]:
        profile = dict(row)
        if profile.get("guild_id") is not None:
            profile["guild_id"] = str(profile["guild_id"])
        profile["display_name"] = profile.get("display_name") or profile.get("username")
        profile["public_profile"] = bool(profile.get("public_profile"))
        profile["email_verified"] = bool(profile.get("email_verified_at"))
        profile["has_password"] = bool(profile.get("password_hash"))
        profile.pop("password_hash", None)
        preferences = profile.get("panel_preferences")
        if isinstance(preferences, str):
            try:
                preferences = json.loads(preferences)
            except Exception:
                preferences = {}
        elif not isinstance(preferences, dict):
            preferences = {}
        profile["panel_preferences"] = preferences
        for key in ("created_at", "last_login_at", "updated_at", "email_verified_at", "email_verification_sent_at"):
            value = profile.get(key)
            if hasattr(value, "isoformat"):
                profile[key] = value.isoformat()
        return profile

    async def get_account_profile(self, username: str, guild_id: str | int) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        row = await self._fetchone(
            f"""
            SELECT username, guild_id, email, email_verified_at, password_hash, display_name, avatar_url, bio, favorite_bot, theme_accent,
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

    async def get_account_admin(self, account_id: int) -> dict[str, Any] | None:
        row = await self._fetchone(
            f"""
            SELECT id, username, guild_id, email, email_verified_at, email_verification_sent_at, password_hash,
                   display_name, avatar_url, bio, favorite_bot, theme_accent, public_profile,
                   server_invite_url, server_name, server_icon_url, panel_preferences,
                   created_at, last_login_at, updated_at
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE id = %s
            LIMIT 1
            """,
            (_coerce_int(account_id, "account_id"),),
        )
        return self._serialize_account_profile(row) if row else None

    async def get_account_admin_data(self, query: str = "", limit: int = 100) -> dict[str, Any]:
        normalized_query = str(query or "").strip()
        safe_limit = max(1, min(200, int(limit or 100)))
        like = f"%{normalized_query}%"
        summary_row = await self._fetchone(
            f"""
            SELECT
                COUNT(*) AS total_accounts,
                SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) AS accounts_with_email,
                SUM(CASE WHEN email_verified_at IS NOT NULL THEN 1 ELSE 0 END) AS verified_emails,
                SUM(CASE WHEN email IS NOT NULL AND email_verified_at IS NULL THEN 1 ELSE 0 END) AS pending_emails,
                SUM(CASE WHEN public_profile = 1 THEN 1 ELSE 0 END) AS public_profiles,
                SUM(CASE WHEN password_hash IS NOT NULL AND password_hash != '' THEN 1 ELSE 0 END) AS passwords_set
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            """,
        ) or {}
        rows = await self._fetchall(
            f"""
            SELECT id, username, guild_id, email, email_verified_at, email_verification_sent_at, password_hash,
                   display_name, favorite_bot, public_profile, server_name,
                   created_at, last_login_at, updated_at
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE (
                %s = ''
                OR username LIKE %s
                OR CAST(guild_id AS CHAR) LIKE %s
                OR COALESCE(email, '') LIKE %s
                OR COALESCE(display_name, '') LIKE %s
                OR COALESCE(server_name, '') LIKE %s
            )
            ORDER BY COALESCE(updated_at, created_at) DESC, username ASC
            LIMIT %s
            """,
            (normalized_query, like, like, like, like, like, safe_limit),
        )
        return {
            "summary": {
                "total_accounts": int(summary_row.get("total_accounts") or 0),
                "accounts_with_email": int(summary_row.get("accounts_with_email") or 0),
                "verified_emails": int(summary_row.get("verified_emails") or 0),
                "pending_emails": int(summary_row.get("pending_emails") or 0),
                "public_profiles": int(summary_row.get("public_profiles") or 0),
                "passwords_set": int(summary_row.get("passwords_set") or 0),
            },
            "users": [self._serialize_account_profile(row) for row in rows],
            "query": normalized_query,
            "limit": safe_limit,
        }

    async def update_account_admin(self, account_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
        safe_account_id = _coerce_int(account_id, "account_id")
        allowed = {"username", "guild_id", "email", "display_name", "public_profile", "server_name"}
        cleaned: dict[str, Any] = {}
        if "username" in updates:
            cleaned["username"] = _normalize_account_username(updates.get("username"))
        if "guild_id" in updates:
            cleaned["guild_id"] = _coerce_int(updates.get("guild_id"), "guild_id")
        if "email" in updates:
            email = _normalize_email(updates.get("email"))
            cleaned["email"] = email
            cleaned["email_verified_at"] = None
            cleaned["email_verification_token_hash"] = None
            cleaned["email_verification_sent_at"] = None
        if "display_name" in updates:
            cleaned["display_name"] = str(updates.get("display_name") or "").strip()[:80] or None
        if "public_profile" in updates:
            cleaned["public_profile"] = 1 if updates.get("public_profile") else 0
        if "server_name" in updates:
            cleaned["server_name"] = str(updates.get("server_name") or "").strip()[:120] or None
        unknown = set(updates) - allowed
        if unknown:
            raise ValueError(f"Unsupported account fields: {', '.join(sorted(unknown))}")

        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await asyncio.wait_for(conn.begin(), timeout=15)
                try:
                    await asyncio.wait_for(cur.execute(
                        f"""
                        SELECT id, username, guild_id, email
                        FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (safe_account_id,),
                    ), timeout=15)
                    current = await asyncio.wait_for(cur.fetchone(), timeout=15)
                    if not current:
                        raise ValueError("SwarmPanel account not found.")

                    next_username = cleaned.get("username", current["username"])
                    next_guild_id = cleaned.get("guild_id", current["guild_id"])
                    next_email = cleaned["email"] if "email" in updates else current.get("email")

                    await asyncio.wait_for(cur.execute(
                        f"""
                        SELECT id, username, guild_id, email
                        FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
                        WHERE id != %s AND (
                            username = %s
                            OR guild_id = %s
                            OR (%s IS NOT NULL AND email = %s)
                        )
                        LIMIT 1
                        """,
                        (safe_account_id, next_username, next_guild_id, next_email, next_email),
                    ), timeout=15)
                    conflict = await asyncio.wait_for(cur.fetchone(), timeout=15)
                    if conflict:
                        if conflict.get("username") == next_username:
                            raise ValueError("That username is already registered.")
                        if next_email and conflict.get("email") == next_email:
                            raise ValueError("That email is already registered.")
                        raise ValueError("That guild ID is already registered to another account.")

                    if cleaned:
                        assignments = ", ".join(
                            f"`{_validate_identifier(key, 'account column')}` = %s" for key in cleaned
                        )
                        await asyncio.wait_for(cur.execute(
                            f"""
                            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
                            SET {assignments}
                            WHERE id = %s
                            """,
                            (*cleaned.values(), safe_account_id),
                        ), timeout=15)

                    if next_guild_id != current["guild_id"]:
                        await asyncio.wait_for(cur.execute(
                            f"DELETE FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` WHERE guild_id = %s",
                            (current["guild_id"],),
                        ), timeout=15)
                        await asyncio.wait_for(cur.execute(
                            f"""
                            INSERT INTO `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` (guild_id, username)
                            VALUES (%s, %s)
                            """,
                            (next_guild_id, next_username),
                        ), timeout=15)
                    elif next_username != current["username"]:
                        await asyncio.wait_for(cur.execute(
                            f"""
                            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}`
                            SET username = %s
                            WHERE guild_id = %s
                            """,
                            (next_username, next_guild_id),
                        ), timeout=15)

                    await asyncio.wait_for(conn.commit(), timeout=15)
                except ValueError:
                    await asyncio.wait_for(conn.rollback(), timeout=15)
                    raise
                except Exception as exc:
                    await asyncio.wait_for(conn.rollback(), timeout=15)
                    message = str(exc).lower()
                    if "duplicate" in message or "1062" in message:
                        if next_email and "email" in message:
                            raise ValueError("That email is already registered.") from exc
                        if "guild" in message:
                            raise ValueError("That guild ID is already registered to another account.") from exc
                        raise ValueError("That username is already registered.") from exc
                    raise
        return await self.get_account_admin(safe_account_id)

    async def delete_account_admin(self, account_id: int) -> None:
        safe_account_id = _coerce_int(account_id, "account_id")
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await asyncio.wait_for(conn.begin(), timeout=15)
                try:
                    await asyncio.wait_for(cur.execute(
                        f"""
                        SELECT guild_id
                        FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (safe_account_id,),
                    ), timeout=15)
                    current = await asyncio.wait_for(cur.fetchone(), timeout=15)
                    if not current:
                        raise ValueError("SwarmPanel account not found.")
                    await asyncio.wait_for(cur.execute(
                        f"DELETE FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}` WHERE id = %s",
                        (safe_account_id,),
                    ), timeout=15)
                    await asyncio.wait_for(cur.execute(
                        f"DELETE FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_GUILD_LOCK_TABLE}` WHERE guild_id = %s",
                        (current["guild_id"],),
                    ), timeout=15)
                    await asyncio.wait_for(conn.commit(), timeout=15)
                except ValueError:
                    await asyncio.wait_for(conn.rollback(), timeout=15)
                    raise
                except Exception:
                    await asyncio.wait_for(conn.rollback(), timeout=15)
                    raise

    async def set_account_email_verified_admin(self, account_id: int, verified: bool) -> dict[str, Any] | None:
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET email_verified_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE NULL END,
                email_verification_token_hash = CASE WHEN %s THEN NULL ELSE email_verification_token_hash END
            WHERE id = %s
            """,
            (1 if verified else 0, 1 if verified else 0, _coerce_int(account_id, "account_id")),
        )
        return await self.get_account_admin(account_id)

    async def update_account_password(self, username: str, guild_id: str | int, current_password: str, new_password: str) -> dict[str, Any] | None:
        username = _normalize_account_username(username)
        gid = _coerce_int(guild_id, "guild_id")
        next_password = _normalize_account_password(new_password, "New password")
        row = await self._fetchone(
            f"""
            SELECT username, guild_id, password_hash
            FROM `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            WHERE username = %s AND guild_id = %s
            LIMIT 1
            """,
            (username, gid),
        )
        if not row:
            return None
        stored_hash = row.get("password_hash")
        current_secret = str(current_password or "")
        valid_current = _verify_password_hash(current_secret, stored_hash) if stored_hash else secrets.compare_digest(str(gid), current_secret.strip())
        if not valid_current:
            raise ValueError("Current password is incorrect.")
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET password_hash = %s
            WHERE username = %s AND guild_id = %s
            """,
            (_account_password_hash(next_password), username, gid),
        )
        return await self.get_account_profile(username, gid)

    async def reset_account_password_admin(self, account_id: int, new_password: str) -> dict[str, Any] | None:
        safe_account_id = _coerce_int(account_id, "account_id")
        normalized_password = _normalize_account_password(new_password, "Password")
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET password_hash = %s
            WHERE id = %s
            """,
            (_account_password_hash(normalized_password), safe_account_id),
        )
        return await self.get_account_admin(safe_account_id)

    async def issue_account_email_verification_token_by_id(self, account_id: int, token: str) -> dict[str, Any] | None:
        token_hash = _verification_token_hash(token)
        await self._execute(
            f"""
            UPDATE `{ACCOUNT_LOGIN_SCHEMA}`.`{ACCOUNT_LOGIN_TABLE}`
            SET email_verification_token_hash = %s, email_verification_sent_at = CURRENT_TIMESTAMP
            WHERE id = %s AND email IS NOT NULL AND email_verified_at IS NULL
            """,
            (token_hash, _coerce_int(account_id, "account_id")),
        )
        return await self.get_account_admin(account_id)

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
        return {
            "top_tracks": [],
            "top_bots": [],
            "active_sessions": [],
            "total_plays": 0,
            "learned_tracks": 0,
            "smart_likes": 0,
            "smart_dislikes": 0,
            "smart_recommendations": 0,
            "top_smart_tracks": [],
        }

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

        intelligence = await self.get_music_intelligence_summary(limit=3, guild_ids=normalized_guilds)
        for bot_summary in intelligence.get("bots", []):
            for guild_row in bot_summary.get("guilds", []):
                guild_id = str(guild_row.get("guild_id"))
                if guild_id not in summaries:
                    continue
                smart = summaries[guild_id]
                smart["learned_tracks"] += int(guild_row.get("learned_tracks") or 0)
                smart["smart_likes"] += int(guild_row.get("likes") or 0)
                smart["smart_dislikes"] += int(guild_row.get("dislikes") or 0)
                smart["smart_recommendations"] += int(guild_row.get("recommendations") or 0)
                for track in bot_summary.get("top_tracks", []):
                    if str(track.get("guild_id")) == guild_id:
                        smart["top_smart_tracks"].append({
                            "bot_key": bot_summary.get("bot_key"),
                            "bot_display": bot_summary.get("bot_display"),
                            "title": track.get("title"),
                            "smart_score": track.get("smart_score"),
                        })
        for summary in summaries.values():
            summary["top_smart_tracks"] = sorted(
                summary["top_smart_tracks"],
                key=lambda item: -float(item.get("smart_score") or 0),
            )[:3]

        return summaries

    async def get_music_intelligence_summary(
        self,
        guild_id: str | int | None = None,
        bot_key: str | None = None,
        limit: int = 8,
        guild_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        safe_limit = max(1, min(int(limit or 8), 25))
        normalized_guild_id = _coerce_int(guild_id, "guild_id") if guild_id not in (None, "") else None
        normalized_guild_ids = sorted({int(gid) for gid in (guild_ids or [])})
        if normalized_guild_id is not None:
            normalized_guild_ids = [normalized_guild_id]
        normalized_bot_key = str(bot_key or "").strip().lower() or None
        if normalized_bot_key and normalized_bot_key not in BOT_INDEX:
            raise ValueError("Unknown bot key")
        bots = [BOT_INDEX[normalized_bot_key]] if normalized_bot_key else list(MUSIC_BOTS)

        cache_guild_key = ",".join(str(gid) for gid in normalized_guild_ids) if normalized_guild_ids else None
        cache_key = (cache_guild_key, normalized_bot_key, safe_limit)
        now = time.monotonic()
        cached = self._music_intelligence_cache.get(cache_key)
        if cached and cached[0] > now:
            return copy.deepcopy(cached[1])

        totals = {
            "learned_tracks": 0,
            "plays": 0,
            "finishes": 0,
            "skips": 0,
            "likes": 0,
            "dislikes": 0,
            "recommendations": 0,
        }
        result_bots: list[dict[str, Any]] = []

        for bot in bots:
            if bot.kind != "music" or not bot.db_schema or not bot.table_prefix:
                continue
            schema = _validate_identifier(bot.db_schema, "schema")
            prefix = _validate_identifier(bot.table_prefix, "table prefix")
            intelligence_table = f"{prefix}_track_intelligence"
            affinity_table = f"{prefix}_user_track_affinity"
            recommendations_table = f"{prefix}_smart_recommendations"
            if not await self._table_exists(schema, intelligence_table):
                continue

            where = ""
            params: tuple[Any, ...] = ()
            if normalized_guild_ids:
                placeholders = ", ".join(["%s"] * len(normalized_guild_ids))
                where = f"WHERE guild_id IN ({placeholders})"
                params = tuple(normalized_guild_ids)

            summary = await self._fetchone(
                f"""
                SELECT COUNT(*) AS learned_tracks,
                       COALESCE(SUM(play_count), 0) AS plays,
                       COALESCE(SUM(finish_count), 0) AS finishes,
                       COALESCE(SUM(skip_count), 0) AS skips,
                       COALESCE(SUM(like_count), 0) AS likes,
                       COALESCE(SUM(dislike_count), 0) AS dislikes
                FROM `{schema}`.`{intelligence_table}`
                {where}
                """,
                params,
            ) or {}
            top_tracks = await self._fetchall(
                f"""
                SELECT guild_id, title, video_url, play_count, finish_count, skip_count, like_count, dislike_count,
                       ((finish_count * 3) + (like_count * 5) + play_count - (skip_count * 2) - (dislike_count * 5)) AS smart_score,
                       updated_at
                FROM `{schema}`.`{intelligence_table}`
                {where}
                ORDER BY smart_score DESC, updated_at DESC
                LIMIT %s
                """,
                (*params, safe_limit),
            )

            guild_rows: list[dict[str, Any]] = []
            if normalized_guild_ids:
                guild_rows = await self._fetchall(
                    f"""
                    SELECT guild_id, COUNT(*) AS learned_tracks,
                           COALESCE(SUM(play_count), 0) AS plays,
                           COALESCE(SUM(like_count), 0) AS likes,
                           COALESCE(SUM(dislike_count), 0) AS dislikes
                    FROM `{schema}`.`{intelligence_table}`
                    {where}
                    GROUP BY guild_id
                    """,
                    params,
                )
            elif summary.get("learned_tracks"):
                guild_rows = await self._fetchall(
                    f"""
                    SELECT guild_id, COUNT(*) AS learned_tracks,
                           COALESCE(SUM(play_count), 0) AS plays,
                           COALESCE(SUM(like_count), 0) AS likes,
                           COALESCE(SUM(dislike_count), 0) AS dislikes
                    FROM `{schema}`.`{intelligence_table}`
                    GROUP BY guild_id
                    ORDER BY learned_tracks DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )

            top_users: list[dict[str, Any]] = []
            if await self._table_exists(schema, affinity_table):
                top_users = await self._fetchall(
                    f"""
                    SELECT guild_id, user_id, COUNT(*) AS track_count, COALESCE(SUM(score), 0) AS taste_score,
                           COALESCE(SUM(like_count), 0) AS likes, COALESCE(SUM(dislike_count), 0) AS dislikes
                    FROM `{schema}`.`{affinity_table}`
                    {where}
                    GROUP BY guild_id, user_id
                    ORDER BY taste_score DESC, likes DESC
                    LIMIT %s
                    """,
                    (*params, safe_limit),
                )

            recommendation_count = 0
            if await self._table_exists(schema, recommendations_table):
                rec_row = await self._fetchone(
                    f"SELECT COUNT(*) AS recommendations FROM `{schema}`.`{recommendations_table}` {where}",
                    params,
                ) or {}
                recommendation_count = int(rec_row.get("recommendations") or 0)
                if guild_rows:
                    rec_rows = await self._fetchall(
                        f"SELECT guild_id, COUNT(*) AS recommendations FROM `{schema}`.`{recommendations_table}` {where} GROUP BY guild_id",
                        params,
                    )
                    rec_map = {int(row["guild_id"]): int(row.get("recommendations") or 0) for row in rec_rows}
                    for row in guild_rows:
                        row["recommendations"] = rec_map.get(int(row["guild_id"]), 0)

            item = {
                "bot_key": bot.key,
                "bot_display": bot.display_name,
                "schema": schema,
                "learned_tracks": int(summary.get("learned_tracks") or 0),
                "plays": int(summary.get("plays") or 0),
                "finishes": int(summary.get("finishes") or 0),
                "skips": int(summary.get("skips") or 0),
                "likes": int(summary.get("likes") or 0),
                "dislikes": int(summary.get("dislikes") or 0),
                "recommendations": recommendation_count,
                "top_tracks": top_tracks,
                "top_users": top_users,
                "guilds": guild_rows,
            }
            result_bots.append(item)
            totals["learned_tracks"] += item["learned_tracks"]
            totals["plays"] += item["plays"]
            totals["finishes"] += item["finishes"]
            totals["skips"] += item["skips"]
            totals["likes"] += item["likes"]
            totals["dislikes"] += item["dislikes"]
            totals["recommendations"] += item["recommendations"]

        result = {
            "guild_id": str(normalized_guild_id) if normalized_guild_id is not None else None,
            "guild_ids": [str(gid) for gid in normalized_guild_ids],
            "bot_key": normalized_bot_key,
            "totals": totals,
            "bots": result_bots,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._music_intelligence_cache[cache_key] = (time.monotonic() + PANEL_MUSIC_INTELLIGENCE_CACHE_TTL_SECONDS, copy.deepcopy(result))
        return result

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
        schema = _validate_identifier(schema, "schema")
        table = _validate_identifier(table, "table")
        key = (schema, table)
        now = time.monotonic()
        cached = self._table_exists_cache.get(key)
        if cached and cached[0] > now:
            return bool(cached[1])
        row = await self._fetchone(
            """
            SELECT 1 AS table_exists
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        exists = bool(row)
        self._table_exists_cache[key] = (now + PANEL_TABLE_CACHE_TTL_SECONDS, exists)
        return exists

    async def ping(self) -> bool:
        try:
            row = await self._fetchone("SELECT 1 AS ok")
            return bool(row and row.get("ok") == 1)
        except Exception:
            return False

    async def list_schemas(self) -> list[str]:
        now = time.monotonic()
        if self._schema_cache and self._schema_cache[0] > now:
            return list(self._schema_cache[1])
        rows = await self._fetchall(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY schema_name
            """
        )
        schemas = [row["schema_name"] for row in rows]
        self._schema_cache = (time.monotonic() + PANEL_SCHEMA_CACHE_TTL_SECONDS, list(schemas))
        return schemas

    async def list_tables(self, schema: str) -> list[dict[str, Any]]:
        schema = _validate_identifier(schema, "schema")
        now = time.monotonic()
        cached = self._tables_cache.get(schema)
        if cached and cached[0] > now:
            return copy.deepcopy(cached[1])
        if cached:
            self._tables_cache.pop(schema, None)
        rows = await self._fetchall(
            """
            SELECT table_name, table_rows
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (schema,),
        )
        tables = [
            {"table_name": row["table_name"], "estimated_rows": int(row["table_rows"] or 0)}
            for row in rows
        ]
        self._tables_cache[schema] = (time.monotonic() + PANEL_TABLE_CACHE_TTL_SECONDS, copy.deepcopy(tables))
        return tables

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
        intelligence_table = f"{prefix}_track_intelligence"
        affinity_table = f"{prefix}_user_track_affinity"
        recommendations_table = f"{prefix}_smart_recommendations"

        table_exists = {
            "playback": await self._table_exists(schema, playback_table),
            "settings": await self._table_exists(schema, settings_table),
            "queue": await self._table_exists(schema, queue_table),
            "backup": await self._table_exists(schema, backup_table),
            "home": await self._table_exists(schema, home_table),
            "direct_orders": await self._table_exists(schema, direct_orders_table),
            "heartbeat": await self._table_exists(schema, heartbeat_table),
            "metrics": await self._table_exists(schema, metrics_table),
            "intelligence": await self._table_exists(schema, intelligence_table),
            "affinity": await self._table_exists(schema, affinity_table),
            "recommendations": await self._table_exists(schema, recommendations_table),
        }

        playback: dict[str, Any] = {}
        settings: dict[str, Any] = {}
        queue_count = 0
        backup_queue_count = 0
        backup_queue_preview: list[dict[str, Any]] = []
        pending_direct_orders = 0
        latest_direct_order: dict[str, Any] | None = None
        home_channel_id: int | None = None
        feedback_channel_id: int | None = None
        heartbeat_age = None
        heartbeat_status = "unknown"
        intelligence = {
            "learned_tracks": 0,
            "plays": 0,
            "likes": 0,
            "dislikes": 0,
            "recommendations": 0,
            "top_seed": None,
            "top_user": None,
        }

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
                f"fade_seconds, fade_curve, custom_speed, custom_pitch, custom_modifiers_left, dj_only_mode, stay_in_vc "
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
            try:
                backup_queue_preview = await self._fetchall(
                    f"SELECT title, video_url, requester_id FROM `{schema}`.`{backup_table}` "
                    f"WHERE guild_id = %s AND bot_name = %s ORDER BY id ASC LIMIT 8",
                    (gid, bot.key),
                )
            except Exception:
                backup_queue_preview = await self._fetchall(
                    f"SELECT title, video_url, requester_id FROM `{schema}`.`{backup_table}` "
                    f"WHERE guild_id = %s ORDER BY id ASC LIMIT 8",
                    (gid,),
                )

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

        if table_exists["intelligence"]:
            row = await self._fetchone(
                f"""
                SELECT COUNT(*) AS learned_tracks,
                       COALESCE(SUM(play_count), 0) AS plays,
                       COALESCE(SUM(like_count), 0) AS likes,
                       COALESCE(SUM(dislike_count), 0) AS dislikes
                FROM `{schema}`.`{intelligence_table}`
                WHERE guild_id = %s
                """,
                (gid,),
            ) or {}
            intelligence.update({
                "learned_tracks": int(row.get("learned_tracks") or 0),
                "plays": int(row.get("plays") or 0),
                "likes": int(row.get("likes") or 0),
                "dislikes": int(row.get("dislikes") or 0),
            })
            top_seed = await self._fetchone(
                f"""
                SELECT title, video_url,
                       ((finish_count * 3) + (like_count * 5) + play_count - (skip_count * 2) - (dislike_count * 5)) AS smart_score
                FROM `{schema}`.`{intelligence_table}`
                WHERE guild_id = %s AND dislike_count <= like_count
                ORDER BY smart_score DESC, updated_at DESC
                LIMIT 1
                """,
                (gid,),
            )
            if top_seed:
                intelligence["top_seed"] = top_seed

        if table_exists["affinity"]:
            top_user = await self._fetchone(
                f"""
                SELECT user_id, COUNT(*) AS track_count, COALESCE(SUM(score), 0) AS taste_score,
                       COALESCE(SUM(like_count), 0) AS likes, COALESCE(SUM(dislike_count), 0) AS dislikes
                FROM `{schema}`.`{affinity_table}`
                WHERE guild_id = %s
                GROUP BY user_id
                ORDER BY taste_score DESC, likes DESC
                LIMIT 1
                """,
                (gid,),
            )
            if top_user:
                intelligence["top_user"] = top_user

        if table_exists["recommendations"]:
            row = await self._fetchone(
                f"SELECT COUNT(*) AS recommendations FROM `{schema}`.`{recommendations_table}` WHERE guild_id = %s",
                (gid,),
            ) or {}
            intelligence["recommendations"] = int(row.get("recommendations") or 0)

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
                "fade_seconds": float(settings.get("fade_seconds") or 5.0),
                "fade_curve": settings.get("fade_curve") or "linear",
                "custom_speed": float(settings.get("custom_speed") or 1.0),
                "custom_pitch": float(settings.get("custom_pitch") or 1.0),
                "custom_modifiers_left": int(settings.get("custom_modifiers_left") or 0),
                "dj_only_mode": bool(settings.get("dj_only_mode")),
                "stay_in_vc": bool(settings.get("stay_in_vc")),
                "queue_count": queue_count,
                "backup_queue_count": backup_queue_count,
                "backup_queue_preview": backup_queue_preview,
                "backup_restore_ready": backup_restore_ready,
                "backup_restore_reason": backup_restore_reason,
                "pending_direct_orders": pending_direct_orders,
                "latest_direct_order": latest_direct_order,
                "home_channel_id": str(home_channel_id) if home_channel_id else None,
                "home_channel_name": None,
                "feedback_channel_id": str(feedback_channel_id) if feedback_channel_id else None,
                "feedback_channel_name": None,
                "intelligence": intelligence,
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
        intelligence_table = f"{prefix}_track_intelligence"
        recommendations_table = f"{prefix}_smart_recommendations"

        table_exists = {
            "playback": await self._table_exists(schema, playback_table),
            "settings": await self._table_exists(schema, settings_table),
            "queue": await self._table_exists(schema, queue_table),
            "backup": await self._table_exists(schema, backup_table),
            "home": await self._table_exists(schema, home_table),
            "heartbeat": await self._table_exists(schema, heartbeat_table),
            "metrics": await self._table_exists(schema, metrics_table),
            "intelligence": await self._table_exists(schema, intelligence_table),
            "recommendations": await self._table_exists(schema, recommendations_table),
        }

        playback_rows: list[dict[str, Any]] = []
        filter_map: dict[int, dict[str, Any]] = {}
        queue_map: dict[int, int] = {}
        backup_queue_map: dict[int, int] = {}
        home_map: dict[int, int | None] = {}
        metrics_map: dict[int, dict[str, Any]] = {}
        intelligence_map: dict[int, dict[str, Any]] = {}
        recommendation_map: dict[int, int] = {}
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
                    "transition_mode": row.get("transition_mode") or "off",
                    "fade_seconds": float(row.get("fade_seconds") or 5.0),
                    "fade_curve": row.get("fade_curve") or "linear",
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

        if table_exists.get("intelligence"):
            try:
                rows = await self._fetchall(
                    f"""
                    SELECT guild_id, COUNT(*) AS learned_tracks,
                           COALESCE(SUM(play_count), 0) AS plays,
                           COALESCE(SUM(like_count), 0) AS likes,
                           COALESCE(SUM(dislike_count), 0) AS dislikes
                    FROM `{schema}`.`{intelligence_table}`
                    GROUP BY guild_id
                    """
                )
                for row in rows:
                    guild_id = int(row["guild_id"])
                    intelligence_map[guild_id] = {
                        "learned_tracks": int(row.get("learned_tracks") or 0),
                        "plays": int(row.get("plays") or 0),
                        "likes": int(row.get("likes") or 0),
                        "dislikes": int(row.get("dislikes") or 0),
                    }
                    known_guilds.add(guild_id)
            except Exception:
                logger.exception("Failed reading music intelligence for %s", bot.key)

        if table_exists.get("recommendations"):
            try:
                rows = await self._fetchall(
                    f"SELECT guild_id, COUNT(*) AS recommendations FROM `{schema}`.`{recommendations_table}` GROUP BY guild_id"
                )
                for row in rows:
                    recommendation_map[int(row["guild_id"])] = int(row.get("recommendations") or 0)
            except Exception:
                logger.exception("Failed reading smart recommendations for %s", bot.key)

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
            smart = dict(intelligence_map.get(guild_id, {}))
            smart["recommendations"] = recommendation_map.get(guild_id, 0)
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
                    "transition_mode": settings.get("transition_mode", "off"),
                    "fade_seconds": settings.get("fade_seconds", 5.0),
                    "fade_curve": settings.get("fade_curve", "linear"),
                    "queue_count": queue_count,
                    "backup_queue_count": backup_queue_count,
                    "backup_restore_ready": bool(backup_queue_count > 0 and session_state in {"recovering", "queued", "configured", "idle", "paused"}),
                    "backup_restore_reason": "Backup queue is armed when the live queue or playback path goes idle." if backup_queue_count > 0 else "No backup queue entries are stored for this guild.",
                    "home_channel_id": str(home_channel_id) if home_channel_id else None,
                    "home_channel_name": None,
                    "guild_name": None,
                    "channel_name": None,
                    "intelligence": smart,
                }
            )

        if sessions:
            thumbnails = await asyncio.gather(
                *(self._get_thumbnail_url(session.get("video_url")) for session in sessions),
                return_exceptions=True,
            )
            for session, thumbnail in zip(sessions, thumbnails):
                session["thumbnail"] = None if isinstance(thumbnail, Exception) else thumbnail

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
            "queue_depth": sum(int(item.get("queue_count") or 0) for item in sessions),
            "backup_queue_depth": sum(int(item.get("backup_queue_count") or 0) for item in sessions),
            "learned_track_count": sum(int(item.get("learned_tracks") or 0) for item in intelligence_map.values()),
            "smart_recommendation_count": sum(recommendation_map.values()),
            "sessions": sessions,
        }

    async def get_stability_snapshot(self) -> dict[str, Any]:
        """Return Aria recovery/degraded-mode status plus bot metric freshness."""
        snapshot: dict[str, Any] = {"cooldowns": [], "recent_repairs": [], "metrics": await self.get_metrics_snapshot(), "status": "ok"}
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    try:
                        await cur.execute("""
                            SELECT scope_key, scope_type, guild_id, bot_name, reason,
                                   TIMESTAMPDIFF(SECOND, NOW(), cooldown_until) AS remaining_seconds,
                                   cooldown_until, updated_at
                            FROM discord_aria.aria_swarm_recovery_cooldowns
                            WHERE cooldown_until > NOW()
                            ORDER BY cooldown_until DESC
                            LIMIT 50
                        """)
                        snapshot["cooldowns"] = await cur.fetchall()
                    except Exception as exc:
                        snapshot["cooldowns_error"] = str(exc)
                    try:
                        await cur.execute("""
                            SELECT issue_type, repair_action, repair_scope, success, confidence, details, error_text, created_at
                            FROM discord_aria.aria_repair_journal
                            ORDER BY created_at DESC
                            LIMIT 50
                        """)
                        snapshot["recent_repairs"] = await cur.fetchall()
                    except Exception as exc:
                        snapshot["repairs_error"] = str(exc)
        except Exception as exc:
            snapshot["status"] = "error"
            snapshot["error"] = str(exc)
        return snapshot

    async def get_dashboard_data(self) -> dict[str, Any]:
        now = time.monotonic()
        if self._dashboard_cache and self._dashboard_cache[0] > now:
            return copy.deepcopy(self._dashboard_cache[1])
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

        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "bots": bots,
        }
        self._dashboard_cache = (time.monotonic() + PANEL_DASHBOARD_CACHE_TTL_SECONDS, copy.deepcopy(result))
        return result



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
        self._invalidate_hot_caches()

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
        self._invalidate_hot_caches()
        return {"schema": schema, "truncated_tables": len(tables), "tables": [t["table_name"] for t in tables]}

    async def get_table_data(self, schema: str, table: str, limit: int = 100) -> dict[str, Any]:
        schema = _validate_identifier(schema, "schema")
        table = _validate_identifier(table, "table")
        if schema in SYSTEM_SCHEMAS:
            raise ValueError(f"Refusing operation on system schema: {schema}")
        safe_limit = max(1, min(int(limit or 100), 500))
        cache_key = (schema, table, safe_limit)
        now = time.monotonic()
        cached = self._table_data_cache.get(cache_key)
        if cached and cached[0] > now:
            self._table_data_cache.move_to_end(cache_key)
            return copy.deepcopy(cached[1])
        if cached:
            self._table_data_cache.pop(cache_key, None)

        await self._ensure_connected()

        async with self.pool.acquire() as conn:
            # Use DictCursor so the frontend gets column names alongside the values
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(f"SELECT * FROM `{schema}`.`{table}` LIMIT %s", (safe_limit,))
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

                result = {
                    "schema": schema,
                    "table": table,
                    "count": len(processed_rows),
                    "rows": processed_rows
                }
                self._table_data_cache[cache_key] = (time.monotonic() + PANEL_TABLE_DATA_CACHE_TTL_SECONDS, copy.deepcopy(result))
                self._table_data_cache.move_to_end(cache_key)
                while len(self._table_data_cache) > PANEL_TABLE_DATA_CACHE_MAX_ITEMS:
                    self._table_data_cache.popitem(last=False)
                return result

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
        now = time.monotonic()
        cached = self._image_gallery_admin_cache.get(safe_limit)
        if cached and cached[0] > now:
            return copy.deepcopy(cached[1])
        if cached:
            self._image_gallery_admin_cache.pop(safe_limit, None)
        await self._ensure_connected()
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                summary: dict[str, Any] = {"schema": schema}
                for key, table in (
                    ("users", "users"),
                    ("media", "media_items"),
                    ("comments", "media_comments"),
                    ("reports_open", "media_reports"),
                    ("collections", "media_collections"),
                ):
                    if key == "reports_open":
                        await cur.execute(f"SELECT COUNT(*) AS count FROM `{schema}`.`{table}` WHERE status='open'")
                    else:
                        await cur.execute(f"SELECT COUNT(*) AS count FROM `{schema}`.`{table}`")
                    row = await cur.fetchone() or {}
                    summary[key] = int(row.get("count") or 0)

                await cur.execute(
                    f"""
                    SELECT u.id, u.username, u.display_name, u.email, u.email_verified_at,
                           u.email_verification_sent_at, u.public_profile, u.show_liked_count,
                           u.birthdate, u.age_verified_at, u.adult_content_consent,
                           u.created_at, u.last_login_at,
                           COUNT(DISTINCT m.id) AS media_count,
                           COUNT(DISTINCT c.id) AS comment_count,
                           COUNT(DISTINCT b.media_id) AS bookmark_count,
                           COUNT(DISTINCT col.id) AS collection_count
                    FROM `{schema}`.`users` u
                    LEFT JOIN `{schema}`.`media_items` m ON m.user_id = u.id
                    LEFT JOIN `{schema}`.`media_comments` c ON c.user_id = u.id
                    LEFT JOIN `{schema}`.`media_bookmarks` b ON b.user_id = u.id
                    LEFT JOIN `{schema}`.`media_collections` col ON col.user_id = u.id
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
                           m.is_adult, m.moderation_status, m.moderation_reason, m.created_at, u.username
                    FROM `{schema}`.`media_items` m
                    JOIN `{schema}`.`users` u ON u.id = m.user_id
                    ORDER BY m.created_at DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                media = [self._json_row(row) for row in await cur.fetchall()]

                await cur.execute(
                    f"""
                    SELECT r.id, r.media_id, r.user_id, r.reason, r.details, r.status, r.created_at,
                           u.username, u.display_name, m.title AS media_title
                    FROM `{schema}`.`media_reports` r
                    JOIN `{schema}`.`users` u ON u.id = r.user_id
                    JOIN `{schema}`.`media_items` m ON m.id = r.media_id
                    ORDER BY FIELD(r.status, 'open', 'reviewed', 'dismissed'), r.created_at DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                reports = [self._json_row(row) for row in await cur.fetchall()]

                await cur.execute(
                    f"""
                    SELECT c.id, c.name, c.slug, c.media_kind, c.created_at,
                           COUNT(m.id) AS media_count
                    FROM `{schema}`.`categories` c
                    LEFT JOIN `{schema}`.`media_items` m ON m.category_id = c.id
                    GROUP BY c.id
                    ORDER BY c.name ASC
                    """
                )
                categories = [self._json_row(row) for row in await cur.fetchall()]

                await cur.execute(
                    f"""
                    SELECT col.id, col.user_id, col.name, col.is_public, col.created_at, u.username,
                           COUNT(ci.media_id) AS item_count
                    FROM `{schema}`.`media_collections` col
                    JOIN `{schema}`.`users` u ON u.id = col.user_id
                    LEFT JOIN `{schema}`.`media_collection_items` ci ON ci.collection_id = col.id
                    GROUP BY col.id
                    ORDER BY col.created_at DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                collections = [self._json_row(row) for row in await cur.fetchall()]
        result = {
            "schema": schema,
            "summary": summary,
            "users": users,
            "comments": comments,
            "media": media,
            "reports": reports,
            "categories": categories,
            "collections": collections,
        }
        self._image_gallery_admin_cache[safe_limit] = (time.monotonic() + PANEL_IMAGE_GALLERY_ADMIN_CACHE_TTL_SECONDS, copy.deepcopy(result))
        return result

    async def get_image_gallery_user_admin(self, user_id: int) -> dict[str, Any] | None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        row = await self._fetchone(
            f"""
            SELECT id, username, display_name, email, email_verified_at, email_verification_sent_at,
                   public_profile, show_liked_count, birthdate, age_verified_at, adult_content_consent,
                   created_at, last_login_at
            FROM `{schema}`.`users`
            WHERE id = %s
            LIMIT 1
            """,
            (_coerce_int(user_id, "user_id"),),
        )
        return self._json_row(row) if row else None

    async def delete_image_gallery_user(self, user_id: int) -> None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        await self._execute(f"DELETE FROM `{schema}`.`users` WHERE id = %s", (_coerce_int(user_id, "user_id"),))
        self._invalidate_hot_caches()

    async def delete_image_gallery_comment(self, comment_id: int) -> None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        await self._execute(f"DELETE FROM `{schema}`.`media_comments` WHERE id = %s", (_coerce_int(comment_id, "comment_id"),))
        self._invalidate_hot_caches()

    async def reset_image_gallery_user_password(self, user_id: int, new_password: str) -> None:
        if len(str(new_password or "")) < 8:
            raise ValueError("Password must be at least 8 characters.")
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        password_hash = _gallery_password_hash(new_password)
        await self._execute(
            f"UPDATE `{schema}`.`users` SET password_hash = %s WHERE id = %s",
            (password_hash, _coerce_int(user_id, "user_id")),
        )
        self._invalidate_hot_caches()

    async def update_image_gallery_user(self, user_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        allowed = {
            "username",
            "display_name",
            "email",
            "public_profile",
            "show_liked_count",
            "adult_content_consent",
        }
        cleaned: dict[str, Any] = {}
        if "username" in updates:
            cleaned["username"] = str(updates["username"] or "").strip()[:40]
            if not cleaned["username"]:
                raise ValueError("Username cannot be empty.")
        if "display_name" in updates:
            cleaned["display_name"] = str(updates["display_name"] or "").strip()[:80] or None
        if "email" in updates:
            email = _normalize_email(updates.get("email"))
            cleaned["email"] = email
            if email:
                cleaned["email_verified_at"] = None
                cleaned["email_verification_token_hash"] = None
                cleaned["email_verification_sent_at"] = None
        for key in ("public_profile", "show_liked_count", "adult_content_consent"):
            if key in updates:
                cleaned[key] = 1 if updates.get(key) else 0
        unknown = set(updates) - allowed
        if unknown:
            raise ValueError(f"Unsupported user fields: {', '.join(sorted(unknown))}")
        if cleaned:
            assignments = ", ".join(f"`{_validate_identifier(key, 'gallery user column')}` = %s" for key in cleaned)
            await self._execute(
                f"UPDATE `{schema}`.`users` SET {assignments} WHERE id = %s",
                (*cleaned.values(), _coerce_int(user_id, "user_id")),
            )
            self._invalidate_hot_caches()
        return await self.get_image_gallery_user_admin(user_id)

    async def set_image_gallery_email_verified(self, user_id: int, verified: bool) -> dict[str, Any] | None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        await self._execute(
            f"""
            UPDATE `{schema}`.`users`
            SET email_verified_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE NULL END,
                email_verification_token_hash = CASE WHEN %s THEN NULL ELSE email_verification_token_hash END
            WHERE id = %s
            """,
            (1 if verified else 0, 1 if verified else 0, _coerce_int(user_id, "user_id")),
        )
        self._invalidate_hot_caches()
        return await self.get_image_gallery_user_admin(user_id)

    async def set_image_gallery_age_verified(self, user_id: int, verified: bool) -> dict[str, Any] | None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        await self._execute(
            f"""
            UPDATE `{schema}`.`users`
            SET age_verified_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE NULL END,
                adult_content_consent = %s
            WHERE id = %s
            """,
            (1 if verified else 0, 1 if verified else 0, _coerce_int(user_id, "user_id")),
        )
        self._invalidate_hot_caches()
        return await self.get_image_gallery_user_admin(user_id)

    async def issue_image_gallery_email_verification_token(self, user_id: int, token: str) -> dict[str, Any] | None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        token_hash = _verification_token_hash(token)
        await self._execute(
            f"""
            UPDATE `{schema}`.`users`
            SET email_verification_token_hash = %s, email_verification_sent_at = CURRENT_TIMESTAMP
            WHERE id = %s AND email IS NOT NULL AND email_verified_at IS NULL
            """,
            (token_hash, _coerce_int(user_id, "user_id")),
        )
        self._invalidate_hot_caches()
        return await self.get_image_gallery_user_admin(user_id)

    async def update_image_gallery_media(self, media_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        allowed = {"title", "is_adult", "moderation_status", "moderation_reason"}
        cleaned: dict[str, Any] = {}
        if "title" in updates:
            title = str(updates["title"] or "").strip()[:160]
            if not title:
                raise ValueError("Media title cannot be empty.")
            cleaned["title"] = title
        if "is_adult" in updates:
            cleaned["is_adult"] = 1 if updates.get("is_adult") else 0
            cleaned["adult_marked_by_user"] = 1 if updates.get("is_adult") else 0
        if "moderation_status" in updates:
            status = str(updates["moderation_status"] or "").strip().lower()
            if status not in {"clear", "review", "blocked"}:
                raise ValueError("Moderation status must be clear, review, or blocked.")
            cleaned["moderation_status"] = status
        if "moderation_reason" in updates:
            cleaned["moderation_reason"] = str(updates["moderation_reason"] or "").strip()[:300] or None
        unknown = set(updates) - allowed
        if unknown:
            raise ValueError(f"Unsupported media fields: {', '.join(sorted(unknown))}")
        if cleaned:
            assignments = ", ".join(f"`{_validate_identifier(key, 'gallery media column')}` = %s" for key in cleaned)
            await self._execute(
                f"UPDATE `{schema}`.`media_items` SET {assignments}, moderated_at=CURRENT_TIMESTAMP WHERE id = %s",
                (*cleaned.values(), _coerce_int(media_id, "media_id")),
            )
            self._invalidate_hot_caches()
        row = await self._fetchone(f"SELECT * FROM `{schema}`.`media_items` WHERE id = %s", (_coerce_int(media_id, "media_id"),))
        return self._json_row(row) if row else None

    async def delete_image_gallery_media(self, media_id: int) -> None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        await self._execute(f"DELETE FROM `{schema}`.`media_items` WHERE id = %s", (_coerce_int(media_id, "media_id"),))
        self._invalidate_hot_caches()

    async def update_image_gallery_report_status(self, report_id: int, status: str) -> None:
        schema = _validate_identifier(self.settings.image_gallery_schema, "image gallery schema")
        status = str(status or "").strip().lower()
        if status not in {"open", "reviewed", "dismissed"}:
            raise ValueError("Report status must be open, reviewed, or dismissed.")
        await self._execute(
            f"UPDATE `{schema}`.`media_reports` SET status = %s WHERE id = %s",
            (status, _coerce_int(report_id, "report_id")),
        )
        self._invalidate_hot_caches()

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

    async def _shuffle_live_queue(self, cur: aiomysql.DictCursor, schema: str, prefix: str, gid: int, bot_key: str) -> int:
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS `{schema}`.`{prefix}_queue` "
            "(id INT AUTO_INCREMENT PRIMARY KEY, guild_id BIGINT, "
            "bot_name VARCHAR(50), video_url TEXT, title TEXT, requester_id BIGINT DEFAULT NULL)"
        )
        await cur.execute(
            f"SELECT * FROM `{schema}`.`{prefix}_queue` WHERE guild_id = %s AND bot_name = %s ORDER BY id ASC",
            (gid, bot_key),
        )
        rows = list(await cur.fetchall() or [])
        if len(rows) <= 1:
            return len(rows)

        first = rows.pop(0)
        random.shuffle(rows)
        rows.insert(0, first)
        cols = [column for column in rows[0].keys() if column != "id"]
        col_names = ", ".join(f"`{column}`" for column in cols)
        placeholders = ", ".join("%s" for _ in cols)

        # Queue leak fix: make panel-side shuffle atomic. The old path deleted
        # the queue and reinserted rows under autocommit, so a DB interruption
        # could drop tracks while the backup still showed they existed.
        try:
            await cur.execute("START TRANSACTION")
            await cur.execute(f"DELETE FROM `{schema}`.`{prefix}_queue` WHERE guild_id = %s AND bot_name = %s", (gid, bot_key))
            for row in rows:
                await cur.execute(
                    f"INSERT INTO `{schema}`.`{prefix}_queue` ({col_names}) VALUES ({placeholders})",
                    tuple(row[column] for column in cols),
                )
            await cur.execute("COMMIT")
        except Exception:
            try:
                await cur.execute("ROLLBACK")
            except Exception:
                pass
            raise
        return len(rows)

    async def _prime_panel_playback_defaults(self, cur: aiomysql.DictCursor, schema: str, prefix: str, gid: int, bot_key: str) -> int:
        await self._ensure_music_guild_settings_schema(cur, schema, prefix)
        await cur.execute(
            f"INSERT INTO `{schema}`.`{prefix}_guild_settings` (guild_id, loop_mode) VALUES (%s, %s) "
            f"ON DUPLICATE KEY UPDATE loop_mode = VALUES(loop_mode)",
            (gid, "queue"),
        )
        return await self._shuffle_live_queue(cur, schema, prefix, gid, bot_key)

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
                    mode = _normalize_loop_mode(payload.get("loop_mode") if isinstance(payload, dict) else payload)
                    await self._ensure_music_guild_settings_schema(cur, schema, prefix)
                    await cur.execute(
                        f"INSERT INTO `{schema}`.`{prefix}_guild_settings` (guild_id, loop_mode) VALUES (%s, %s) "
                        f"ON DUPLICATE KEY UPDATE loop_mode = VALUES(loop_mode)",
                        (gid, mode),
                    )
                    result["loop_mode"] = mode
                    result["message"] = f"Loop mode set to {mode} for guild {gid} on {bot.display_name}."

                elif action == "FILTER":
                    mode = _normalize_filter_mode(payload.get("filter_mode") if isinstance(payload, dict) else payload)
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
                    shuffled_count = await self._shuffle_live_queue(cur, schema, prefix, gid, bot_key)
                    result["queue_count"] = shuffled_count
                    result["message"] = f"Shuffled {shuffled_count} queued tracks for guild {gid} on {bot.display_name}."

                elif action == "SMART_RECOMMEND":
                    if not isinstance(payload, dict):
                        raise ValueError("SMART_RECOMMEND payload must be an object with voice_channel_id")
                    await self._clear_pending_orders(cur, schema, prefix, gid, bot_key)
                    await self._ensure_music_intelligence_schema(cur, schema, prefix)
                    voice_channel_id = _coerce_int(payload.get("voice_channel_id"), "voice_channel_id")
                    text_channel_raw = payload.get("text_channel_id")
                    text_channel_id = _coerce_int(text_channel_raw, "text_channel_id") if text_channel_raw not in (None, "", 0, "0") else 0
                    requester_raw = payload.get("requester_id")
                    requester_id = _coerce_int(requester_raw, "requester_id") if requester_raw not in (None, "", 0, "0") else None
                    shuffled_count = await self._prime_panel_playback_defaults(cur, schema, prefix, gid, bot_key)

                    seed = None
                    reason = "server_favorite"
                    if requester_id:
                        await cur.execute(
                            f"""
                            SELECT title, video_url, score
                            FROM `{schema}`.`{prefix}_user_track_affinity`
                            WHERE guild_id = %s AND user_id = %s AND dislike_count <= like_count
                            ORDER BY score DESC, last_requested DESC
                            LIMIT 1
                            """,
                            (gid, requester_id),
                        )
                        seed = await cur.fetchone()
                        if seed:
                            reason = "personal_taste"
                    if not seed:
                        await cur.execute(
                            f"""
                            SELECT title, video_url,
                                   ((finish_count * 3) + (like_count * 5) + play_count - (skip_count * 2) - (dislike_count * 5)) AS smart_score
                            FROM `{schema}`.`{prefix}_track_intelligence`
                            WHERE guild_id = %s AND dislike_count <= like_count
                            ORDER BY smart_score DESC, updated_at DESC
                            LIMIT 1
                            """,
                            (gid,),
                        )
                        seed = await cur.fetchone()
                    if not seed:
                        raise ValueError("No smart recommendation seed exists for this bot and guild yet")

                    seed_title = str(seed.get("title") or seed.get("video_url") or "").strip()
                    query_text = f"ytmsearch:{_smart_query_from_title(seed_title)} radio"
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
                        (bot_key, gid, voice_channel_id, text_channel_id, "PLAY", query_text),
                    )
                    await cur.execute(
                        f"""
                        INSERT INTO `{schema}`.`{prefix}_smart_recommendations`
                        (guild_id, requester_id, seed_title, seed_url, query_text, reason)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (gid, requester_id, seed_title, seed.get("video_url"), query_text, reason),
                    )
                    result["seed_title"] = seed_title
                    result["query_text"] = query_text
                    result["reason"] = reason
                    result["loop_mode"] = "queue"
                    result["shuffled_queue_count"] = shuffled_count
                    result["message"] = f"Queued a smart recommendation for {bot.display_name} in guild {gid} using {seed_title[:120]}."

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
                    shuffled_count = await self._prime_panel_playback_defaults(cur, schema, prefix, gid, bot_key)

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
                    result["loop_mode"] = "queue"
                    result["shuffled_queue_count"] = shuffled_count
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
        self._invalidate_hot_caches()
        return result
