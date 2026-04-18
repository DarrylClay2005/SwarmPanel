from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import time
from datetime import datetime, timezone
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
            return {
                "status": "online",
                "reachable": True,
                "message": "Connected successfully.",
                "database": row.get("database_name") or database,
                "host": host,
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

        panel_db_probe = await self._probe_mysql(
            host=self.settings.db_host,
            user=self.settings.db_user,
            password=self.settings.db_password,
            database=self.settings.db_default_schema,
        )

        aria_db_probe, aria_swarm_probe, aria_discord_probe, gemini_probe = await asyncio.gather(
            self._probe_mysql(
                host=aria_env["db_host"],
                user=aria_env["db_user"],
                password=aria_env["db_password"],
                database=aria_env["db_name"],
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
            bot_tasks.append(
                asyncio.gather(
                    self._probe_mysql(
                        host=cfg["db_host"],
                        user=cfg["db_user"],
                        password=cfg["db_password"],
                        database=cfg["db_name"],
                    ),
                    self._probe_discord_identity(self.settings.bot_tokens.get(bot.key, "")),
                )
            )

        bot_results_raw = await asyncio.gather(*bot_tasks) if bot_tasks else []
        bot_results = []
        for bot, (db_probe, discord_probe) in zip(MUSIC_BOTS, bot_results_raw):
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
                    },
                    "db": db_probe,
                    "discord": discord_probe,
                    "capabilities": list(MUSIC_PANEL_ACTIONS),
                }
            )

        shared_env_summary = {
            "status": self._status_from_bool(env_found, ok_label="online", fail_label="missing"),
            "found": env_found,
            "path": str(env_path),
            "last_modified": datetime.fromtimestamp(env_path.stat().st_mtime, timezone.utc).isoformat() if env_found else None,
            "message": "Shared Music/.env loaded for diagnostics." if env_found else "Shared Music/.env was not found.",
        }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cache_ttl_seconds": DIAGNOSTICS_CACHE_TTL_SECONDS,
            "shared_env": shared_env_summary,
            "panel": {
                "db": panel_db_probe,
            },
            "aria": {
                "env": {
                    "shared_token_present": bool(aria_env["token"]),
                    "db_password_present": bool(aria_env["db_password"]),
                    "swarm_db_password_present": bool(aria_env["swarm_db_password"]),
                    "gemini_key_present": bool(aria_env["gemini_api_key"]),
                    "panel_token_present": bool(self.settings.bot_tokens.get("aria")),
                },
                "db": aria_db_probe,
                "swarm_db": aria_swarm_probe,
                "discord": aria_discord_probe,
                "gemini": gemini_probe,
                "operator_actions": list(ARIA_OPERATOR_ACTIONS),
            },
            "bots": bot_results,
        }
