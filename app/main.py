import asyncio
import base64
import json
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from typing import Any

from fastapi import FastAPI, Form, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, model_validator
from starlette.middleware.sessions import SessionMiddleware

from .auth import (
    SESSION_AUTH_KEY,
    SESSION_GUILD_ID_KEY,
    SESSION_ROLE_KEY,
    SESSION_USERNAME_KEY,
    get_api_auth,
    is_authenticated,
    issue_api_token,
    require_api_auth,
    verify_api_token,
    verify_credentials,
)
from .bots import (
    ALL_BOTS,
    BOT_ACCENTS,
    BOT_CAPABILITY_SUMMARIES,
    BOT_INDEX,
    MUSIC_BOTS,
    invite_url_for_bot,
    permission_value,
    permissions_for_bot,
)
from .config import load_settings
from .database import PanelDatabase
from .diagnostics import RuntimeDiagnosticsService
from .discord_api import DiscordInventoryService


BASE_DIR = Path(__file__).resolve().parent
settings = load_settings()
db = PanelDatabase(settings)
discord_service = DiscordInventoryService()
diagnostics_service = RuntimeDiagnosticsService(settings, discord_service)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
action_logger = logging.getLogger("swarm_panel.actions")
background_tasks: list[asyncio.Task[Any]] = []
VOICE_CHANNEL_TYPES = {2, 13}
TEXT_CHANNEL_TYPES = {0, 5}
VALID_ACTIONS = {"PAUSE", "RESUME", "SKIP", "STOP", "CLEAR", "SHUFFLE", "LOOP", "PLAY", "RESTART", "FILTER", "LEAVE", "SET_HOME", "RECOVER"}


def _validate_discord_webhook_url(value: Any) -> str:
    url = str(value or "").strip()
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if parsed.scheme != "https" or host not in {"discord.com", "www.discord.com", "discordapp.com", "www.discordapp.com"}:
        raise ValueError("Invalid Discord webhook URL")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 4 or parts[0] != "api" or parts[1] != "webhooks":
        raise ValueError("Invalid Discord webhook URL")
    return url

class TruncateTableRequest(BaseModel):
    schema_name: str
    table_name: str
    confirm_text: str


class TruncateSchemaRequest(BaseModel):
    schema_name: str
    confirm_text: str


class SessionLoginRequest(BaseModel):
    username: str
    password: str
    guild_id: str | int | None = None


class SessionRegisterRequest(BaseModel):
    username: str
    guild_id: str | int


def _feed_event(level: str, title: str, description: str, *, source: str = "panel", event_type: str = "feed_event") -> dict[str, str]:
    return {
        "type": event_type,
        "level": level,
        "title": title,
        "description": description,
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _get_api_auth(request: Request) -> dict[str, Any] | None:
    return get_api_auth(
        request,
        secret_key=settings.session_secret,
        max_age_seconds=settings.api_token_ttl_seconds,
    )


def _require_api_auth(request: Request) -> dict[str, Any]:
    return require_api_auth(
        request,
        secret_key=settings.session_secret,
        max_age_seconds=settings.api_token_ttl_seconds,
    )


def _is_admin_auth(auth: dict[str, Any] | None) -> bool:
    if not auth:
        return False
    return str(auth.get("role") or "admin").lower() == "admin" and not auth.get("guild_id")


def _scoped_guild_id(auth: dict[str, Any] | None) -> str | None:
    if _is_admin_auth(auth):
        return None
    guild_id = auth.get("guild_id") if auth else None
    return str(guild_id) if guild_id not in (None, "") else None


def _client_id_from_token(token: str) -> str | None:
    token_head = str(token or "").split(".", 1)[0].strip()
    if not token_head:
        return None
    try:
        return base64.urlsafe_b64decode(token_head + ("=" * (-len(token_head) % 4))).decode().strip()
    except Exception:
        return None


def _require_admin_auth(request: Request) -> dict[str, Any]:
    auth = _require_api_auth(request)
    if not _is_admin_auth(auth):
        raise HTTPException(status_code=403, detail="Admin access required")
    return auth


def _require_guild_scope(auth: dict[str, Any], guild_id: str | int | None) -> None:
    scoped = _scoped_guild_id(auth)
    if scoped and str(guild_id) != scoped:
        raise HTTPException(status_code=403, detail="This account can only control its registered guild")


def _set_admin_session(request: Request, username: str) -> None:
    request.session[SESSION_AUTH_KEY] = True
    request.session[SESSION_USERNAME_KEY] = username
    request.session[SESSION_ROLE_KEY] = "admin"
    request.session.pop(SESSION_GUILD_ID_KEY, None)


def _set_account_session(request: Request, username: str, guild_id: str | int) -> None:
    request.session[SESSION_AUTH_KEY] = True
    request.session[SESSION_USERNAME_KEY] = username
    request.session[SESSION_ROLE_KEY] = "account"
    request.session[SESSION_GUILD_ID_KEY] = str(guild_id)


async def _authenticate_login(username: str, password: str = "", guild_id: str | int | None = None) -> dict[str, Any] | None:
    if verify_credentials(username, password, settings.admin_username, settings.admin_password):
        return {"username": username, "role": "admin", "guild_id": None}

    account_guild_id = guild_id if guild_id not in (None, "") else password
    if account_guild_id in (None, ""):
        return None
    try:
        account = await db.authenticate_account_login(username, account_guild_id)
    except ValueError:
        return None
    if not account:
        return None
    return {"username": account["username"], "role": "account", "guild_id": account["guild_id"]}


async def _bot_has_registered_guild(bot_key: str, guild_id: str | int) -> bool:
    bot = BOT_INDEX.get(bot_key)
    if not bot or bot.kind != "music":
        return False

    try:
        known_guilds = await db.get_known_guild_ids(bot_key)
        if int(guild_id) in known_guilds:
            return True
    except Exception:
        pass

    token = settings.bot_tokens.get(bot_key, "").strip()
    if not token:
        return False
    try:
        guilds = await discord_service.fetch_guilds(token)
        return any(str(guild.get("id")) == str(guild_id) for guild in guilds)
    except Exception:
        return False


async def _visible_music_bots_for_auth(auth: dict[str, Any]) -> list:
    scoped = _scoped_guild_id(auth)
    if not scoped:
        return list(MUSIC_BOTS)
    checks = await asyncio.gather(
        *(_bot_has_registered_guild(bot.key, scoped) for bot in MUSIC_BOTS),
        return_exceptions=True,
    )
    return [bot for bot, ok in zip(MUSIC_BOTS, checks) if ok is True]


async def _ensure_registerable_guild(guild_id: str | int) -> str:
    try:
        guild_id_text = str(int(str(guild_id).strip()))
    except (TypeError, ValueError):
        raise ValueError("Guild ID must be a Discord server ID number.") from None
    return guild_id_text


async def _require_bot_guild_access(auth: dict[str, Any], bot_key: str, guild_id: str | int) -> None:
    _require_guild_scope(auth, guild_id)
    scoped = _scoped_guild_id(auth)
    if scoped and not await _bot_has_registered_guild(bot_key, scoped):
        raise HTTPException(status_code=403, detail="This bot is not registered with your guild")


def _coerce_control_int(value: Any, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {field_name}: {value!r}") from None


def _normalize_control_action(value: str) -> str:
    action = str(value or "").strip().upper()
    if action not in VALID_ACTIONS:
        raise ValueError(f"Unsupported action: {value}")
    return action


async def _normalize_bot_control_request(req: "BotControlRequest") -> tuple[str, str, Any | None]:
    action = _normalize_control_action(req.action)

    if action == "RESTART":
        return action, "0", req.payload

    guild_id = _coerce_control_int(req.guild_id, "guild_id")
    normalized_payload = req.payload
    if isinstance(normalized_payload, dict):
        for key, value in list(normalized_payload.items()):
            if "webhook" in str(key).lower() and value:
                normalized_payload[key] = _validate_discord_webhook_url(value)

    if action not in {"PLAY", "SET_HOME"}:
        return action, str(guild_id), normalized_payload

    bot = BOT_INDEX.get(req.bot_key)
    if not bot:
        raise ValueError("Unknown bot key")

    token = settings.bot_tokens.get(req.bot_key, "").strip()
    if not token:
        raise ValueError(f"Missing Discord token for {bot.display_name}; cannot validate the selected guild/channel route.")

    if not isinstance(req.payload, dict):
        expected = "source_url and voice_channel_id" if action == "PLAY" else "voice_channel_id"
        raise ValueError(f"{action} payload must be an object with {expected}")

    try:
        guild = await discord_service.fetch_guild(token, guild_id)
        channels = await discord_service.fetch_guild_channels(token, guild_id)
    except Exception as exc:
        raise ValueError(f"{bot.display_name} cannot validate guild {guild_id} via Discord: {exc}") from exc

    channel_map = {int(channel["id"]): channel for channel in channels}
    voice_channel_id = _coerce_control_int(req.payload.get("voice_channel_id"), "voice_channel_id")
    voice_channel = channel_map.get(voice_channel_id)
    if not voice_channel or int(voice_channel.get("type", -1)) not in VOICE_CHANNEL_TYPES:
        guild_name = guild.get("name") or f"Guild {guild_id}"
        raise ValueError(
            f"Voice channel {voice_channel_id} is not a voice/stage channel visible to {bot.display_name} in {guild_name}."
        )

    normalized_payload = dict(req.payload)
    normalized_payload["voice_channel_id"] = voice_channel_id

    if action == "PLAY":
        source_url = str(req.payload.get("source_url") or req.payload.get("query") or "").strip()
        if not source_url:
            raise ValueError("Missing source_url for PLAY action")

        text_channel_raw = req.payload.get("text_channel_id")
        text_channel_id = 0
        if text_channel_raw not in (None, "", 0, "0"):
            text_channel_id = _coerce_control_int(text_channel_raw, "text_channel_id")
            text_channel = channel_map.get(text_channel_id)
            if not text_channel or int(text_channel.get("type", -1)) not in TEXT_CHANNEL_TYPES:
                guild_name = guild.get("name") or f"Guild {guild_id}"
                raise ValueError(
                    f"Text channel {text_channel_id} is not a text/announcement channel visible to {bot.display_name} in {guild_name}."
                )

        normalized_payload = {
            "source_url": source_url,
            "voice_channel_id": voice_channel_id,
            "text_channel_id": text_channel_id,
        }

    return action, str(guild_id), normalized_payload


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await db.connect()
    except Exception:
        logging.getLogger("swarm_panel").exception("Initial DB connection failed at startup; endpoints will retry lazily.")
    await discord_service.connect()
    await diagnostics_service.connect()
    background_tasks.clear()
    # Commands execute directly through /api/bots/control. The old command_queue
    # worker is intentionally not started because no endpoint enqueues work.
    recent_feed_events.append(
        _feed_event(
            "info",
            "Event Feed Ready",
            "SwarmPanel live event feed is online.",
            source="system",
        )
    )
    yield
    for task in background_tasks:
        task.cancel()
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
    background_tasks.clear()
    await diagnostics_service.close()
    await discord_service.close()
    await db.close()


app = FastAPI(title="SwarmPanel", version="1.0.0", lifespan=lifespan)
if settings.cors_allowed_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
app.add_middleware(SessionMiddleware, secret_key=settings.session_secret, max_age=60 * 60 * 12)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _redirect_login() -> RedirectResponse:
    return RedirectResponse(url="/login", status_code=303)


@app.get("/login")
async def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(request, "login.html", {"error": None, "register_error": None})


@app.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(""),
    guild_id: str = Form(""),
):
    auth = await _authenticate_login(username, password, guild_id)
    if auth:
        if auth["role"] == "admin":
            _set_admin_session(request, auth["username"])
        else:
            _set_account_session(request, auth["username"], auth["guild_id"])
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"error": "Invalid username or guild ID", "register_error": None},
        status_code=401,
    )


@app.post("/register")
async def register_submit(request: Request, username: str = Form(...), guild_id: str = Form(...)):
    try:
        registerable_guild_id = await _ensure_registerable_guild(guild_id)
        account = await db.register_account_login(username, registerable_guild_id)
    except ValueError as exc:
        return templates.TemplateResponse(
            request,
            "login.html",
            {"error": None, "register_error": str(exc)},
            status_code=400,
        )
    except Exception as exc:
        return templates.TemplateResponse(
            request,
            "login.html",
            {"error": None, "register_error": f"Registration failed: {exc}"},
            status_code=503,
        )

    _set_account_session(request, account["username"], account["guild_id"])
    return RedirectResponse(url="/", status_code=303)


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return _redirect_login()


@app.get("/")
async def index(request: Request):
    if not is_authenticated(request):
        return _redirect_login()
    return templates.TemplateResponse(request, "index.html", {
        "session_username": request.session.get(SESSION_USERNAME_KEY) or settings.admin_username,
        "session_role": request.session.get(SESSION_ROLE_KEY) or "admin",
        "session_guild_id": request.session.get(SESSION_GUILD_ID_KEY),
    })


@app.get("/api/session")
async def api_session_status(request: Request):
    auth = _get_api_auth(request)
    if not auth:
        return {"authenticated": False, "pages_public_url": settings.pages_public_url}

    username = auth.get("username") or settings.admin_username
    role = auth.get("role") or "admin"
    guild_id = auth.get("guild_id")
    return {
        "authenticated": True,
        "mode": auth.get("mode") or "token",
        "username": username,
        "role": role,
        "guild_id": str(guild_id) if guild_id else None,
        "token": issue_api_token(settings.session_secret, username, role=role, guild_id=guild_id),
        "pages_public_url": settings.pages_public_url,
        "expires_in": settings.api_token_ttl_seconds,
    }


@app.post("/api/session/login")
async def api_session_login(request: Request, payload: SessionLoginRequest):
    auth = await _authenticate_login(payload.username, payload.password, payload.guild_id)
    if not auth:
        raise HTTPException(status_code=401, detail="Invalid username, password, or guild ID")

    if auth["role"] == "admin":
        _set_admin_session(request, auth["username"])
    else:
        _set_account_session(request, auth["username"], auth["guild_id"])
    token = issue_api_token(
        settings.session_secret,
        auth["username"],
        role=auth["role"],
        guild_id=auth.get("guild_id"),
    )
    return {
        "ok": True,
        "token": token,
        "username": auth["username"],
        "role": auth["role"],
        "guild_id": auth.get("guild_id"),
        "pages_public_url": settings.pages_public_url,
        "expires_in": settings.api_token_ttl_seconds,
    }


@app.post("/api/session/register")
async def api_session_register(request: Request, payload: SessionRegisterRequest):
    try:
        registerable_guild_id = await _ensure_registerable_guild(payload.guild_id)
        account = await db.register_account_login(payload.username, registerable_guild_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    _set_account_session(request, account["username"], account["guild_id"])
    token = issue_api_token(
        settings.session_secret,
        account["username"],
        role="account",
        guild_id=account["guild_id"],
    )
    return {
        "ok": True,
        "token": token,
        "username": account["username"],
        "role": "account",
        "guild_id": account["guild_id"],
        "pages_public_url": settings.pages_public_url,
        "expires_in": settings.api_token_ttl_seconds,
    }


@app.post("/api/session/logout")
async def api_session_logout(request: Request):
    request.session.clear()
    return {"ok": True}


@app.get("/api/health")
async def api_health(request: Request):
    _require_api_auth(request)
    return {"ok": True}


@app.get("/api/bots")
async def list_bots(request: Request):
    auth = _require_api_auth(request)
    scoped_guild_id = _scoped_guild_id(auth)
    visible_music_bots = await _visible_music_bots_for_auth(auth)
    visible_bots = [*visible_music_bots]
    if _is_admin_auth(auth):
        visible_bots.append(BOT_INDEX["aria"])
    visible_keys = {bot.key for bot in visible_bots}

    async def invite_payload(bot) -> dict[str, Any]:
        token = settings.bot_tokens.get(bot.key, "")
        client_id = _client_id_from_token(token)
        identity: dict[str, Any] = {}
        if token:
            try:
                identity = await discord_service.fetch_identity(token)
                client_id = identity.get("id") or client_id
            except Exception as exc:
                identity = {"error": str(exc)}
        permissions = permissions_for_bot(bot)
        permission_integer = permission_value(permissions)
        return {
            "key": bot.key,
            "display_name": bot.display_name,
            "name": bot.display_name,
            "kind": bot.kind,
            "schema": bot.db_schema,
            "token_configured": bool(token),
            "client_id": client_id,
            "invite_url": invite_url_for_bot(client_id, permission_integer, scoped_guild_id) if client_id else None,
            "permission_integer": str(permission_integer),
            "permissions": permissions,
            "capability_summary": BOT_CAPABILITY_SUMMARIES.get(bot.kind, "Discord bot with slash commands and server tools."),
            "accent": BOT_ACCENTS.get(bot.key, "#89b4fa"),
            "connected_to_session_guild": bool(scoped_guild_id and bot.key in visible_keys),
            "icon_url": identity.get("avatar_url"),
            "identity_name": identity.get("global_name") or identity.get("username"),
            "identity_error": identity.get("error"),
        }

    return {
        "bots": [
            {
                "key": bot.key,
                "display_name": bot.display_name,
                "name": bot.display_name,
                "kind": bot.kind,
                "schema": bot.db_schema,
                "token_configured": bool(settings.bot_tokens.get(bot.key)),
            }
            for bot in visible_bots
        ],
        # Build identity-backed invite cards concurrently so the panel does not
        # wait on nine Discord identity requests in series.
        "invite_bots": list(await asyncio.gather(*(invite_payload(bot) for bot in ALL_BOTS))),
        "scoped_guild_id": scoped_guild_id,
    }


@app.get("/api/dashboard")
async def dashboard_data(request: Request):
    auth = _require_api_auth(request)
    scoped_guild_id = _scoped_guild_id(auth)
    try:
        data = await db.get_dashboard_data()
    except Exception as exc:
        data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_error": str(exc),
            "bots": [
                {
                    "key": bot.key,
                    "display_name": bot.display_name,
                    "kind": bot.kind,
                    "schema": bot.db_schema,
                    "status": "db-unavailable",
                    "heartbeat_age_seconds": None,
                    "heartbeat_status": "unknown",
                    "active_playing_count": 0,
                    "known_guild_count": 0,
                    "sessions": [],
                }
                for bot in ALL_BOTS
            ],
        }

    if scoped_guild_id:
        visible_bot_keys = {bot.key for bot in await _visible_music_bots_for_auth(auth)}
        scoped_bots = []
        for bot in data.get("bots", []):
            if bot.get("kind") != "music" or bot.get("key") not in visible_bot_keys:
                continue
            sessions = [
                session for session in bot.get("sessions", [])
                if str(session.get("guild_id")) == scoped_guild_id
            ]
            bot["sessions"] = sessions
            bot["known_guild_count"] = 1
            bot["guild_count"] = 1
            bot["active_playing_count"] = sum(1 for session in sessions if session.get("is_playing"))
            scoped_bots.append(bot)
        data["bots"] = scoped_bots

    flattened_sessions: list[dict[str, Any]] = []
    seen_session_keys: set[tuple[str, str]] = set()
    for bot in data["bots"]:
        bot.setdefault("name", bot.get("display_name"))
        bot.setdefault("guild_count", bot.get("known_guild_count", 0))
        token = settings.bot_tokens.get(bot["key"], "")
        bot["discord"] = {"token_configured": bool(token)}
        if not token:
            continue

        try:
            bot["discord"]["identity"] = await discord_service.fetch_identity(token)
        except Exception as exc:
            bot["discord"]["error"] = str(exc)

        sessions = bot.get("sessions", [])
        for session in sessions:
            session.setdefault("bot_key", bot.get("key"))
            session.setdefault("bot_name", bot.get("display_name"))
            session_key = (str(session.get("bot_key") or bot.get("key")), str(session.get("guild_id") or "0"))
            if session_key in seen_session_keys:
                continue
            seen_session_keys.add(session_key)
            flattened_sessions.append(session)
        if not sessions:
            continue
        placements = []
        for session in sessions:
            guild_id = str(session["guild_id"])
            if session.get("channel_id"):
                placements.append((guild_id, str(session["channel_id"])))
            if session.get("home_channel_id"):
                placements.append((guild_id, str(session["home_channel_id"])))
        try:
            name_map = await discord_service.resolve_guild_channel_names(token, placements)
            for session in sessions:
                guild_id = str(session["guild_id"])
                channel_key = (guild_id, str(session["channel_id"])) if session.get("channel_id") else None
                home_key = (guild_id, str(session["home_channel_id"])) if session.get("home_channel_id") else None

                channel_names = name_map.get(channel_key) if channel_key else None
                if channel_names:
                    session["guild_name"] = channel_names.get("guild_name")
                    session["channel_name"] = channel_names.get("channel_name")

                home_names = name_map.get(home_key) if home_key else None
                if home_names:
                    session["guild_name"] = session.get("guild_name") or home_names.get("guild_name")
                    session["home_channel_name"] = home_names.get("channel_name")
        except Exception as exc:
            bot["discord"]["name_resolution_error"] = str(exc)

    data["sessions"] = flattened_sessions
    return data


@app.get("/api/system-diagnostics")
async def system_diagnostics(request: Request, force: bool = False):
    _require_admin_auth(request)
    try:
        return await diagnostics_service.get_snapshot(force=force)
    except Exception as exc:
        action_logger.exception("Failed collecting diagnostics: %s", exc)
        raise HTTPException(status_code=503, detail=f"Diagnostics unavailable: {exc}")


@app.get("/api/bots/{bot_key}/inventory")
async def bot_inventory(request: Request, bot_key: str, include_channels: bool = True):
    auth = _require_api_auth(request)
    bot = BOT_INDEX.get(bot_key)
    if not bot:
        raise HTTPException(status_code=404, detail="Unknown bot key")
    scoped_guild_id = _scoped_guild_id(auth)
    if scoped_guild_id:
        await _require_bot_guild_access(auth, bot_key, scoped_guild_id)

    token = settings.bot_tokens.get(bot_key, "")
    if not token:
        raise HTTPException(status_code=400, detail=f"Missing token env for {bot.display_name}")

    guild_hints = []
    if bot.kind == "music":
        try:
            guild_hints = await db.get_known_guild_ids(bot_key)
        except Exception:
            guild_hints = []
    inventory = await discord_service.fetch_inventory(
        token,
        include_channels=include_channels,
        guild_hints=[scoped_guild_id] if scoped_guild_id else guild_hints,
    )
    if scoped_guild_id:
        inventory["guilds"] = [
            guild for guild in inventory.get("guilds", [])
            if str(guild.get("id")) == scoped_guild_id
        ]
    return {
        "bot": {"key": bot.key, "display_name": bot.display_name, "kind": bot.kind},
        **inventory,
    }


def _control_state_error_payload(bot_key: str, display_name: str, guild_id: int, message: str) -> dict[str, Any]:
    return {
        "key": bot_key,
        "display_name": display_name,
        "guild_id": str(guild_id),
        "db": {
            "status": "error",
            "reachable": False,
            "message": message,
        },
        "discord": {
            "status": "unknown",
            "reachable": False,
            "message": "Discord state was not resolved because the live control query failed.",
            "token_configured": bool(settings.bot_tokens.get(bot_key)),
        },
        "session": {
            "guild_id": str(guild_id),
            "guild_name": None,
            "channel_id": None,
            "channel_name": None,
            "title": None,
            "video_url": None,
            "position_seconds": 0,
            "is_playing": False,
            "session_state": "idle",
            "session_state_label": "Idle",
            "volume": 100,
            "loop_mode": "queue",
            "filter_mode": "none",
            "transition_mode": "off",
            "custom_speed": 1.0,
            "custom_pitch": 1.0,
            "custom_modifiers_left": 0,
            "dj_only_mode": False,
            "stay_in_vc": False,
            "queue_count": 0,
            "backup_queue_count": 0,
            "backup_restore_ready": False,
            "pending_direct_orders": 0,
            "latest_direct_order": None,
            "home_channel_id": None,
            "home_channel_name": None,
            "feedback_channel_id": None,
            "feedback_channel_name": None,
        },
    }


async def _enrich_control_state_with_discord(control_state: dict[str, Any]) -> dict[str, Any]:
    bot_key = control_state["key"]
    guild_id = str(control_state["guild_id"])
    token = settings.bot_tokens.get(bot_key, "").strip()
    session = control_state.get("session", {})

    if not token:
        control_state["discord"] = {
            "status": "missing",
            "reachable": False,
            "message": "Panel token is not configured for Discord inventory access.",
            "token_configured": False,
        }
        return control_state

    try:
        guild = await discord_service.fetch_guild(token, guild_id)
        placements = [(guild_id, None)]
        for channel_id in (
            session.get("channel_id"),
            session.get("home_channel_id"),
            session.get("feedback_channel_id"),
        ):
            if channel_id:
                placements.append((guild_id, str(channel_id)))

        name_map = await discord_service.resolve_guild_channel_names(token, placements)
        guild_meta = name_map.get((guild_id, None), {})
        session["guild_name"] = guild_meta.get("guild_name") or guild.get("name") or f"Guild {guild_id}"

        if session.get("channel_id"):
            session["channel_name"] = (name_map.get((guild_id, str(session["channel_id"]))) or {}).get("channel_name")
        if session.get("home_channel_id"):
            session["home_channel_name"] = (name_map.get((guild_id, str(session["home_channel_id"]))) or {}).get("channel_name")
        if session.get("feedback_channel_id"):
            session["feedback_channel_name"] = (name_map.get((guild_id, str(session["feedback_channel_id"]))) or {}).get("channel_name")

        control_state["discord"] = {
            "status": "online",
            "reachable": True,
            "message": f"Live Discord route is valid in {session['guild_name']}.",
            "token_configured": True,
        }
    except Exception as exc:
        control_state["discord"] = {
            "status": "error",
            "reachable": False,
            "message": str(exc)[:240],
            "token_configured": True,
        }

    return control_state


@app.get("/api/bots/{bot_key}/control-state")
async def bot_control_state(request: Request, bot_key: str, guild_id: str):
    auth = _require_api_auth(request)
    await _require_bot_guild_access(auth, bot_key, guild_id)
    bot = BOT_INDEX.get(bot_key)
    if not bot or bot.kind != "music":
        raise HTTPException(status_code=404, detail="Unknown music bot key")

    try:
        state = await db.get_bot_control_state(bot_key, guild_id)
        return await _enrich_control_state_with_discord(state)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        action_logger.exception("Failed building control state bot=%s guild=%s: %s", bot_key, guild_id, exc)
        raise HTTPException(status_code=503, detail=f"Control state unavailable: {exc}")


@app.get("/api/guilds/{guild_id}/control-matrix")
async def guild_control_matrix(request: Request, guild_id: str):
    auth = _require_api_auth(request)
    _require_guild_scope(auth, guild_id)
    visible_bots = await _visible_music_bots_for_auth(auth)

    async def collect(bot) -> dict[str, Any]:
        try:
            state = await db.get_bot_control_state(bot.key, guild_id)
            return await _enrich_control_state_with_discord(state)
        except Exception as exc:
            action_logger.warning("Failed control matrix snapshot bot=%s guild=%s: %s", bot.key, guild_id, exc)
            return _control_state_error_payload(bot.key, bot.display_name, guild_id, str(exc))

    bots = await asyncio.gather(*(collect(bot) for bot in visible_bots))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "guild_id": str(guild_id),
        "bots": bots,
    }


@app.get("/api/databases")
async def databases(request: Request, include_tables: bool = False):
    _require_admin_auth(request)
    try:
        schemas = await db.list_schemas()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")
    if not include_tables:
        return {"schemas": schemas}

    output: list[dict[str, Any]] = []
    for schema in schemas:
        output.append({"schema": schema, "tables": await db.list_tables(schema)})
    return {"schemas": output}


@app.get("/api/databases/{schema}/tables")
async def tables(request: Request, schema: str):
    _require_admin_auth(request)
    try:
        return {"schema": schema, "tables": await db.list_tables(schema)}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


@app.post("/api/database/truncate-table")
async def truncate_table(request: Request, payload: TruncateTableRequest):
    _require_admin_auth(request)
    expected_confirmation = f"TRUNCATE {payload.schema_name}.{payload.table_name}"
    if payload.confirm_text.strip() != expected_confirmation:
        raise HTTPException(
            status_code=400,
            detail=f"Confirmation mismatch. Expected exact text: {expected_confirmation}",
        )
    try:
        await db.truncate_table(payload.schema_name, payload.table_name)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")
    action_logger.warning("truncate_table schema=%s table=%s", payload.schema_name, payload.table_name)
    return {"ok": True, "message": f"Truncated {payload.schema_name}.{payload.table_name}"}


@app.post("/api/database/truncate-schema")
async def truncate_schema(request: Request, payload: TruncateSchemaRequest):
    _require_admin_auth(request)
    expected_confirmation = f"TRUNCATE ALL {payload.schema_name}"
    if payload.confirm_text.strip() != expected_confirmation:
        raise HTTPException(
            status_code=400,
            detail=f"Confirmation mismatch. Expected exact text: {expected_confirmation}",
        )
    try:
        result = await db.truncate_schema(payload.schema_name)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")
    action_logger.warning("truncate_schema schema=%s tables=%s", payload.schema_name, result["truncated_tables"])
    return {"ok": True, **result}


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.get("/api/database/data")
async def get_table_data(
    request: Request, 
    schema_name: str, 
    table_name: str, 
    limit: int = 100
):
    _require_admin_auth(request)
    try:
        data = await db.get_table_data(schema_name, table_name, limit)
        return {"ok": True, "data": data}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        action_logger.error("Failed to fetch table data schema=%s table=%s: %s", schema_name, table_name, exc)
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")

class BotControlRequest(BaseModel):
    bot_key: str
    guild_id: str | int | None = None
    action: str | None = None
    command: str | None = None
    payload: Any | None = None

    @model_validator(mode="after")
    def _sync_action_aliases(self):
        normalized_action = (self.action or self.command or "").strip()
        if not normalized_action:
            raise ValueError("Missing action")
        self.action = normalized_action
        self.command = normalized_action
        if self.guild_id in (None, ""):
            if normalized_action.upper() == "RESTART":
                self.guild_id = "0"
            else:
                raise ValueError("guild_id is required for non-RESTART actions")
        return self

@app.post("/api/bots/control")
async def api_bot_control(request: Request, req: BotControlRequest):
    auth = _require_api_auth(request)
    try:
        action_name = _normalize_control_action(req.action or req.command or "")
        if _scoped_guild_id(auth) and action_name == "RESTART":
            raise HTTPException(status_code=403, detail="Restart is admin-only because it affects the whole bot node")
        await _require_bot_guild_access(auth, req.bot_key, req.guild_id)
        action, guild_id, payload = await _normalize_bot_control_request(req)
        result = await db.control_bot(req.bot_key, guild_id, action, payload)
        result.setdefault("command", action)
        await push_feed_event(
            "info",
            "Bot Control Accepted",
            result.get("message") or f"{action} accepted for {req.bot_key} in guild {guild_id}.",
            source="api",
            event_type="command_ack",
        )
        return {"ok": True, **result}
    except ValueError as e:
        await push_feed_event("warning", "Invalid Bot Control", str(e), source="api")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        await push_feed_event("error", "Bot Control Failed", str(e), source="api")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/api/control")
async def api_bot_control_legacy(request: Request, req: BotControlRequest):
    return await api_bot_control(request, req)


@app.post("/api/bot-control")
async def api_bot_control_legacy_alt(request: Request, req: BotControlRequest):
    return await api_bot_control(request, req)
active_connections=[]
bot_health={}
recent_feed_events: deque[dict[str, str]] = deque(maxlen=100)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token")
    auth = verify_api_token(token, settings.session_secret, settings.api_token_ttl_seconds)
    if not auth:
        try:
            if bool(websocket.session.get(SESSION_AUTH_KEY)):
                auth = {"mode": "session"}
        except Exception:
            auth = None
    if not auth:
        await websocket.close(code=4401)
        return
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        try:
            active_connections.remove(websocket)
        except ValueError:
            pass

async def broadcast(data: dict):
    dead=[]
    for ws in list(active_connections):
        try:
            await ws.send_text(json.dumps(data))
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            active_connections.remove(ws)
        except ValueError:
            pass


async def push_feed_event(
    level: str,
    title: str,
    description: str,
    *,
    source: str = "panel",
    event_type: str = "feed_event",
) -> dict[str, str]:
    event = _feed_event(level, title, description, source=source, event_type=event_type)
    recent_feed_events.append(event)
    await broadcast(event)
    return event




@app.get("/api/metrics")
async def metrics_data(request: Request):
    _require_admin_auth(request)
    try:
        return await db.get_metrics_snapshot()
    except Exception as exc:
        await push_feed_event("error", "Metrics Snapshot Failed", str(exc), source="api")
        return JSONResponse(
            status_code=503,
            content={
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "db_error": str(exc),
                "totals": {},
                "bots": [],
            },
        )


@app.get("/api/events")
async def list_events(request: Request, limit: int = 50):
    _require_admin_auth(request)
    bounded_limit = max(1, min(int(limit), 100))
    events = list(recent_feed_events)
    try:
        bot_error_events = await db.get_recent_bot_error_events(limit=bounded_limit)
    except Exception:
        bot_error_events = []
    try:
        aria_medic_events = await db.get_recent_aria_medic_events(limit=max(5, bounded_limit // 2))
    except Exception:
        aria_medic_events = []

    combined = events + bot_error_events + aria_medic_events
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for event in sorted(combined, key=lambda item: item.get("timestamp") or ""):
        key = (
            str(event.get("timestamp") or ""),
            str(event.get("source") or ""),
            str(event.get("title") or ""),
            str(event.get("description") or ""),
            str(event.get("type") or "feed_event"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(event)
    return {"events": deduped[-bounded_limit:]}

async def execute_bot_command(cmd: dict):
    if not cmd.get("bot_key") or not cmd.get("action"):
        raise ValueError(f"Invalid command: {cmd}")

    req = BotControlRequest(**{"guild_id": "0", **cmd})
    action, guild_id, payload = await _normalize_bot_control_request(req)
    await db.control_bot(req.bot_key, guild_id, action, payload)
    await push_feed_event(
        "info",
        "Command Acknowledged",
        f"{action} accepted for bot {req.bot_key} in guild {guild_id}.",
        source="worker",
        event_type="command_ack",
    )

async def command_worker():
    # Retained for compatibility with old imports, but intentionally disabled.
    # The live panel dispatches bot commands synchronously through api_bot_control.
    return

def verify_token(token: str = Header(None)):
    if token != os.getenv("PANEL_API_KEY"):
        raise HTTPException(403,"Unauthorized")
