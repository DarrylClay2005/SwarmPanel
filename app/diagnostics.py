from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any

import aiohttp
import aiomysql
from dotenv import dotenv_values

from .bots import MUSIC_BOTS
from .config import Settings
from .discord_api import DiscordInventoryService


logger = logging.getLogger("swarm_panel")
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SHARED_ENV_FILE = REPO_ROOT / "DC" / ".env"
DIAGNOSTICS_CACHE_TTL_SECONDS = 90
CENTRAL_TZ = ZoneInfo("America/Chicago")

MUSIC_PANEL_ACTIONS = [
    {"id": "PLAY", "label": "Queue track or playlist", "scope": "bot + guild + voice channel"},
    {"id": "PAUSE", "label": "Pause playback", "scope": "bot + guild"},
    {"id": "RESUME", "label": "Resume playback", "scope": "bot + guild"},
    {"id": "SKIP", "label": "Skip current track", "scope": "bot + guild"},
    {"id": "STOP", "label": "Stop playback", "scope": "bot + guild"},
    {"id": "LEAVE", "label": "Disconnect from voice", "scope": "bot + guild"},
    {"id": "CLEAR", "label": "Clear queue and current track", "scope": "bot + guild"},
    {"id": "SHUFFLE", "label": "Shuffle queued tracks", "scope": "bot + guild"},
    {"id": "LOOP", "label": "Change loop mode", "scope": "bot + guild"},
    {"id": "FILTER", "label": "Apply audio filter", "scope": "bot + guild"},
    {"id": "SET_HOME", "label": "Set default home voice channel", "scope": "bot + guild + voice channel"},
    {"id": "RESTART", "label": "Restart node", "scope": "bot"},
]

ARIA_OPERATOR_ACTIONS = [
    "Route playback through any swarm node",
    "Broadcast a track to the whole swarm",
    "Set bot home channels",
    "Change loop and filter settings",
    "Issue pause, resume, skip, stop, and leave orders",
]


class RuntimeDiagnosticsService:
    def __init__(self, settings: Settings, discord_service: DiscordInventoryService):
        self.settings = settings
        self.discord_service = discord_service
        self.http_session: aiohttp.ClientSession | None = None
        self._cache: dict[str, Any] | None = None
        self._cache_expires_at = 0.0
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        if self.http_session:
            return
        timeout = aiohttp.ClientTimeout(total=10)
        self.http_session = aiohttp.ClientSession(timeout=timeout)
        await self.discord_service.connect()

    async def close(self) -> None:
        if self.http_session:
            await self.http_session.close()
            self.http_session = None
        await self.discord_service.close()

    async def get_snapshot(self, *, force: bool = False) -> dict[str, Any]:
        now = time.monotonic()
        if not force and self._cache and now < self._cache_expires_at:
            return self._cache

        async with self._lock:
            now = time.monotonic()
            if not force and self._cache and now < self._cache_expires_at:
                return self._cache

            snapshot = await self._build_snapshot()
            self._cache = snapshot
            self._cache_expires_at = time.monotonic() + DIAGNOSTICS_CACHE_TTL_SECONDS
            return snapshot

    def _resolve_shared_env_path(self) -> Path:
        return DEFAULT_SHARED_ENV_FILE

    def _load_shared_env(self) -> tuple[Path, dict[str, str]]:
        env_path = self._resolve_shared_env_path()
        if not env_path.is_file():
            return env_path, {}

        raw_values = dotenv_values(env_path)
        values: dict[str, str] = {}
        for key, value in raw_values.items():
            if key:
                values[str(key).strip()] = str(value or "").strip()
        return env_path, values

    @staticmethod
    def _status_from_bool(value: bool, *, ok_label: str = "online", fail_label: str = "missing") -> str:
        return ok_label if value else fail_label

    @staticmethod
    def _mask_secret(value: str) -> str:
        value = str(value or "")
        if not value:
            return "missing"
        if len(value) <= 8:
            return "present"
        return f"{value[:3]}…{value[-3:]}"

    @staticmethod
    def _format_central(iso_value: str | None) -> str | None:
        if not iso_value:
            return None
        try:
            return datetime.fromisoformat(iso_value.replace("Z", "+00:00")).astimezone(CENTRAL_TZ).isoformat()
        except Exception:
            return iso_value


    def _music_env_config(self, bot_key: str, env_values: dict[str, str]) -> dict[str, Any]:
        prefix = bot_key.upper()
        return {
            "token": env_values.get(f"{prefix}_DISCORD_TOKEN", ""),
            "db_host": env_values.get(f"{prefix}_DB_HOST", "127.0.0.1"),
            "db_user": env_values.get(f"{prefix}_DB_USER", "botuser"),
            "db_password": env_values.get(f"{prefix}_DB_PASSWORD", ""),
            "db_name": env_values.get(f"{prefix}_DB_NAME", f"discord_music_{bot_key}"),
            "lavalink_password": env_values.get(f"{prefix}_LAVALINK_PASSWORD", ""),
        }

    def _aria_env_config(self, env_values: dict[str, str]) -> dict[str, Any]:
        return {
            "token": env_values.get("ARIA_DISCORD_TOKEN", ""),
            "db_host": env_values.get("ARIA_DB_HOST", "127.0.0.1"),
            "db_user": env_values.get("ARIA_DB_USER", "botuser"),
            "db_password": env_values.get("ARIA_DB_PASSWORD", ""),
            "db_name": env_values.get("ARIA_DB_NAME", "discord_aria"),
            "swarm_db_host": env_values.get("ARIA_SWARM_DB_HOST", "127.0.0.1"),
            "swarm_db_user": env_values.get("ARIA_SWARM_DB_USER", "botuser"),
            "swarm_db_password": env_values.get("ARIA_SWARM_DB_PASSWORD", env_values.get("ARIA_DB_PASSWORD", "")),
            "swarm_db_name": env_values.get("ARIA_SWARM_DB_NAME", "discord_music_gws"),
            "gemini_api_key": env_values.get("GEMINI_API_KEY", ""),
            "gemini_model": env_values.get("ARIA_GEMINI_MODEL", env_values.get("GEMINI_MODEL", "gemini-2.5-flash")).strip() or "gemini-2.5-flash",
        }

    async def _probe_mysql(
        self,
        *,
        host: str,
        user: str,
        password: str,
        database: str,
    ) -> dict[str, Any]:
        if not host or not user or not database:
            return {
                "status": "missing",
                "reachable": False,
                "message": "Database host, user, or schema is missing.",
                "database": database or None,
                "host": host or None,
            }

        if not password:
            return {
                "status": "missing",
                "reachable": False,
                "message": "Database password is missing.",
                "database": database,
                "host": host,
            }

        conn = None
        try:
            conn = await aiomysql.connect(
                host=host,
                user=user,
                password=password,
                db=database,
                autocommit=True,
                connect_timeout=5,
            )
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT DATABASE() AS database_name, 1 AS ok")
                row = await cur.fetchone() or {}
                write_ok = False
                write_message = "Read-only probe only."
                try:
                    await cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS panel_diag_write_probe (id INT PRIMARY KEY AUTO_INCREMENT, note VARCHAR(32))")
                    await cur.execute("INSERT INTO panel_diag_write_probe (note) VALUES ('ok')")
                    await cur.execute("DROP TEMPORARY TABLE IF EXISTS panel_diag_write_probe")
                    write_ok = True
                    write_message = "Temporary write probe succeeded."
                except Exception as write_exc:
                    write_message = str(write_exc)[:240]
            return {
                "status": "online",
                "reachable": True,
                "message": "Connected successfully.",
                "database": row.get("database_name") or database,
                "host": host,
                "write_ok": write_ok,
                "write_message": write_message,
            }
        except Exception as exc:
            return {
                "status": "error",
                "reachable": False,
                "message": str(exc)[:240],
                "database": database,
                "host": host,
            }
        finally:
            if conn is not None:
                conn.close()

    async def _collect_schema_details(
        self,
        *,
        host: str,
        user: str,
        password: str,
        database: str,
        table_names: list[str],
        count_tables: list[str] | None = None,
        extra_queries: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if not host or not user or not database or not password:
            return {"table_count": 0, "tables_present": [], "missing_tables": table_names, "row_counts": {}, "extras": {}}

        conn = None
        count_tables = count_tables or []
        extra_queries = extra_queries or {}
        try:
            conn = await aiomysql.connect(
                host=host, user=user, password=password, db=database, autocommit=True, connect_timeout=5
            )
            async with conn.cursor(aiomysql.DictCursor) as cur:
                placeholders = ",".join(["%s"] * len(table_names))
                await cur.execute(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_name IN ({placeholders})",
                    [database, *table_names],
                )
                present_rows = await cur.fetchall() or []
                present = [str(r.get("table_name")) for r in present_rows if r.get("table_name")]
                missing = [name for name in table_names if name not in present]
                row_counts: dict[str, int | None] = {}
                for table_name in count_tables:
                    if table_name not in present:
                        row_counts[table_name] = None
                        continue
                    try:
                        await cur.execute(f"SELECT COUNT(*) AS c FROM `{table_name}`")
                        row = await cur.fetchone() or {}
                        row_counts[table_name] = int(row.get("c") or 0)
                    except Exception:
                        row_counts[table_name] = None
                extras: dict[str, Any] = {}
                for key, query in extra_queries.items():
                    try:
                        await cur.execute(query)
                        row = await cur.fetchone() or {}
                        extras[key] = next(iter(row.values())) if row else None
                    except Exception:
                        extras[key] = None
                return {
                    "table_count": len(present),
                    "tables_present": present,
                    "missing_tables": missing,
                    "row_counts": row_counts,
                    "extras": extras,
                }
        except Exception:
            return {"table_count": 0, "tables_present": [], "missing_tables": table_names, "row_counts": {}, "extras": {}}
        finally:
            if conn is not None:
                conn.close()

    async def _probe_discord_identity(self, token: str) -> dict[str, Any]:
        if not token:
            return {
                "status": "missing",
                "reachable": False,
                "message": "Panel token is not configured for Discord inventory access.",
                "identity": None,
            }

        try:
            identity = await self.discord_service.fetch_identity(token)
            label = identity.get("username") or identity.get("global_name") or identity.get("id")
            return {
                "status": "online",
                "reachable": True,
                "message": f"Discord API identity resolved as {label}.",
                "identity": identity,
            }
        except Exception as exc:
            return {
                "status": "error",
                "reachable": False,
                "message": str(exc)[:240],
                "identity": None,
            }

    async def _probe_gemini(self, api_key: str, model_id: str) -> dict[str, Any]:
        sdk_installed = importlib.util.find_spec("google.genai") is not None
        result = {
            "status": "missing",
            "reachable": False,
            "message": "Gemini key is not configured.",
            "model": model_id,
            "sdk_installed": sdk_installed,
            "model_available": None,
        }

        if not api_key:
            return result

        if not self.http_session:
            return {
                **result,
                "status": "error",
                "message": "Diagnostics HTTP session is not initialized.",
            }

        try:
            async with self.http_session.get(
                "https://generativelanguage.googleapis.com/v1beta/models",
                params={"key": api_key},
                headers={"Accept": "application/json"},
            ) as resp:
                body = await resp.text()
                if resp.status >= 400:
                    message = body
                    try:
                        payload = json.loads(body)
                        message = payload.get("error", {}).get("message") or body
                    except Exception:
                        pass
                    return {
                        **result,
                        "status": "error",
                        "message": message[:240],
                    }

                payload = json.loads(body)
                models = [str(item.get("name", "")) for item in payload.get("models", []) if item.get("name")]
                normalized_names = {name.split("/")[-1] for name in models}
                model_available = model_id in normalized_names
                return {
                    "status": "online",
                    "reachable": True,
                    "message": "Gemini API accepted the configured key.",
                    "model": model_id,
                    "sdk_installed": sdk_installed,
                    "model_available": model_available,
                }
        except Exception as exc:
            logger.exception("Gemini diagnostics probe failed: %s", exc)
            return {
                **result,
                "status": "error",
                "message": str(exc)[:240],
            }

    async def _build_snapshot(self) -> dict[str, Any]:
        env_path, env_values = self._load_shared_env()
        env_found = env_path.is_file()
        aria_env = self._aria_env_config(env_values)

        panel_db_probe, panel_db_details, aria_db_probe, aria_db_details, aria_swarm_probe, aria_discord_probe, gemini_probe = await asyncio.gather(
            self._probe_mysql(
                host=self.settings.db_host,
                user=self.settings.db_user,
                password=self.settings.db_password,
                database=self.settings.db_default_schema,
            ),
            self._collect_schema_details(
                host=self.settings.db_host,
                user=self.settings.db_user,
                password=self.settings.db_password,
                database=self.settings.db_default_schema,
                table_names=["panel_events", "panel_notes", "panel_preferences"],
                count_tables=["panel_events"],
            ),
            self._probe_mysql(
                host=aria_env["db_host"],
                user=aria_env["db_user"],
                password=aria_env["db_password"],
                database=aria_env["db_name"],
            ),
            self._collect_schema_details(
                host=aria_env["db_host"],
                user=aria_env["db_user"],
                password=aria_env["db_password"],
                database=aria_env["db_name"],
                table_names=["aria_interactions", "aria_swarm_events", "aria_repair_tasks", "aria_infra_tasks", "aria_operator_decisions", "aria_swarm_health"],
                count_tables=["aria_interactions", "aria_swarm_events", "aria_repair_tasks", "aria_infra_tasks", "aria_operator_decisions", "aria_swarm_health"],
            ),
            self._probe_mysql(
                host=aria_env["swarm_db_host"],
                user=aria_env["swarm_db_user"],
                password=aria_env["swarm_db_password"],
                database=aria_env["swarm_db_name"],
            ),
            self._probe_discord_identity(self.settings.bot_tokens.get("aria", "")),
            self._probe_gemini(aria_env["gemini_api_key"], aria_env["gemini_model"]),
        )

        bot_tasks = []
        bot_env_payloads: dict[str, dict[str, Any]] = {}
        for bot in MUSIC_BOTS:
            cfg = self._music_env_config(bot.key, env_values)
            bot_env_payloads[bot.key] = cfg
            table_prefix = bot.key
            bot_tasks.append(
                asyncio.gather(
                    self._probe_mysql(
                        host=cfg["db_host"],
                        user=cfg["db_user"],
                        password=cfg["db_password"],
                        database=cfg["db_name"],
                    ),
                    self._probe_discord_identity(self.settings.bot_tokens.get(bot.key, "")),
                    self._collect_schema_details(
                        host=cfg["db_host"],
                        user=cfg["db_user"],
                        password=cfg["db_password"],
                        database=cfg["db_name"],
                        table_names=[
                            f"{table_prefix}_playback_state",
                            f"{table_prefix}_queue",
                            f"{table_prefix}_queue_backup",
                            f"{table_prefix}_swarm_overrides",
                            f"{table_prefix}_swarm_direct_orders",
                            f"{table_prefix}_bot_home_channels",
                        ],
                        count_tables=[
                            f"{table_prefix}_playback_state",
                            f"{table_prefix}_queue",
                            f"{table_prefix}_queue_backup",
                            f"{table_prefix}_swarm_overrides",
                            f"{table_prefix}_swarm_direct_orders",
                            f"{table_prefix}_bot_home_channels",
                        ],
                        extra_queries={
                            "active_playback": f"SELECT COUNT(*) AS c FROM `{table_prefix}_playback_state` WHERE is_playing = 1",
                            "paused_playback": f"SELECT COUNT(*) AS c FROM `{table_prefix}_playback_state` WHERE is_playing = 0 AND position_seconds > 0",
                        },
                    ),
                )
            )

        bot_results_raw = await asyncio.gather(*bot_tasks) if bot_tasks else []
        bot_results = []
        for bot, (db_probe, discord_probe, db_details) in zip(MUSIC_BOTS, bot_results_raw):
            cfg = bot_env_payloads[bot.key]
            bot_results.append(
                {
                    "key": bot.key,
                    "display_name": bot.display_name,
                    "env": {
                        "shared_token_present": bool(cfg["token"]),
                        "shared_db_password_present": bool(cfg["db_password"]),
                        "shared_lavalink_password_present": bool(cfg["lavalink_password"]),
                        "panel_token_present": bool(self.settings.bot_tokens.get(bot.key)),
                        "masked_token": self._mask_secret(cfg["token"]),
                        "masked_db_password": self._mask_secret(cfg["db_password"]),
                        "masked_lavalink_password": self._mask_secret(cfg["lavalink_password"]),
                        "db_name": cfg["db_name"],
                        "db_host": cfg["db_host"],
                    },
                    "db": db_probe,
                    "db_details": db_details,
                    "discord": discord_probe,
                    "capabilities": list(MUSIC_PANEL_ACTIONS),
                }
            )

        shared_env_summary = {
            "status": self._status_from_bool(env_found, ok_label="online", fail_label="missing"),
            "found": env_found,
            "path": str(env_path),
            "last_modified": datetime.fromtimestamp(env_path.stat().st_mtime, timezone.utc).isoformat() if env_found else None,
            "last_modified_central": self._format_central(datetime.fromtimestamp(env_path.stat().st_mtime, timezone.utc).isoformat()) if env_found else None,
            "message": "Shared Music/.env loaded for diagnostics." if env_found else "Shared Music/.env was not found.",
        }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cache_ttl_seconds": DIAGNOSTICS_CACHE_TTL_SECONDS,
            "shared_env": shared_env_summary,
            "panel": {
                "db": panel_db_probe,
                "db_details": panel_db_details,
            },
            "aria": {
                "env": {
                    "shared_token_present": bool(aria_env["token"]),
                    "db_password_present": bool(aria_env["db_password"]),
                    "swarm_db_password_present": bool(aria_env["swarm_db_password"]),
                    "gemini_key_present": bool(aria_env["gemini_api_key"]),
                    "panel_token_present": bool(self.settings.bot_tokens.get("aria")),
                    "masked_token": self._mask_secret(aria_env["token"]),
                    "masked_db_password": self._mask_secret(aria_env["db_password"]),
                    "masked_swarm_db_password": self._mask_secret(aria_env["swarm_db_password"]),
                    "masked_gemini_key": self._mask_secret(aria_env["gemini_api_key"]),
                    "db_name": aria_env["db_name"],
                    "db_host": aria_env["db_host"],
                    "swarm_db_name": aria_env["swarm_db_name"],
                    "swarm_db_host": aria_env["swarm_db_host"],
                },
                "db": aria_db_probe,
                "db_details": aria_db_details,
                "swarm_db": aria_swarm_probe,
                "discord": aria_discord_probe,
                "gemini": gemini_probe,
                "operator_actions": list(ARIA_OPERATOR_ACTIONS),
            },
            "bots": bot_results,
        }
