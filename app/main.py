import asyncio
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

from .auth import SESSION_AUTH_KEY, is_authenticated, require_api_auth, verify_credentials
from .bots import ALL_BOTS, BOT_INDEX, MUSIC_BOTS
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


class TruncateTableRequest(BaseModel):
    schema_name: str
    table_name: str
    confirm_text: str


class TruncateSchemaRequest(BaseModel):
    schema_name: str
    confirm_text: str


def _feed_event(level: str, title: str, description: str, *, source: str = "panel", event_type: str = "feed_event") -> dict[str, str]:
    return {
        "type": event_type,
        "level": level,
        "title": title,
        "description": description,
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


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
    except Exception as exc:
        logging.getLogger("swarm_panel").warning("Initial DB connection failed at startup: %s", exc)
    await discord_service.connect()
    await diagnostics_service.connect()
    background_tasks.clear()
    background_tasks.extend(
        [
            asyncio.create_task(command_worker(), name="swarm_panel_command_worker"),
            asyncio.create_task(monitor_bots(), name="swarm_panel_monitor_bots"),
        ]
    )
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
app.add_middleware(SessionMiddleware, secret_key=settings.session_secret, max_age=60 * 60 * 12)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _redirect_login() -> RedirectResponse:
    return RedirectResponse(url="/login", status_code=303)


@app.get("/login")
async def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(request, "login.html", {"error": None})


@app.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    if verify_credentials(username, password, settings.admin_username, settings.admin_password):
        request.session[SESSION_AUTH_KEY] = True
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"error": "Invalid username or password"},
        status_code=401,
    )


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return _redirect_login()


@app.get("/")
async def index(request: Request):
    if not is_authenticated(request):
        return _redirect_login()
    return templates.TemplateResponse(request, "index.html", {})


@app.get("/api/health")
async def api_health(request: Request):
    require_api_auth(request)
    return {"ok": True}


@app.get("/api/bots")
async def list_bots(request: Request):
    require_api_auth(request)
    return {
        "bots": [
            {
                "key": bot.key,
                "display_name": bot.display_name,
                "kind": bot.kind,
                "schema": bot.db_schema,
                "token_configured": bool(settings.bot_tokens.get(bot.key)),
            }
            for bot in ALL_BOTS
        ]
    }


@app.get("/api/dashboard")
async def dashboard_data(request: Request):
    require_api_auth(request)
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

    for bot in data["bots"]:
        token = settings.bot_tokens.get(bot["key"], "")
        bot["discord"] = {"token_configured": bool(token)}
        if not token:
            continue

        try:
            bot["discord"]["identity"] = await discord_service.fetch_identity(token)
        except Exception as exc:
            bot["discord"]["error"] = str(exc)

        sessions = bot.get("sessions", [])
        if not sessions:
            continue
        placements = []
        for session in sessions:
            guild_id = int(session["guild_id"])
            if session.get("channel_id"):
                placements.append((guild_id, int(session["channel_id"])))
            if session.get("home_channel_id"):
                placements.append((guild_id, int(session["home_channel_id"])))
        try:
            name_map = await discord_service.resolve_guild_channel_names(token, placements)
            for session in sessions:
                guild_id = int(session["guild_id"])
                channel_key = (guild_id, int(session["channel_id"])) if session.get("channel_id") else None
                home_key = (guild_id, int(session["home_channel_id"])) if session.get("home_channel_id") else None

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

    return data


@app.get("/api/system-diagnostics")
async def system_diagnostics(request: Request, force: bool = False):
    require_api_auth(request)
    try:
        return await diagnostics_service.get_snapshot(force=force)
    except Exception as exc:
        action_logger.exception("Failed collecting diagnostics: %s", exc)
        raise HTTPException(status_code=503, detail=f"Diagnostics unavailable: {exc}")


@app.get("/api/bots/{bot_key}/inventory")
async def bot_inventory(request: Request, bot_key: str, include_channels: bool = True):
    require_api_auth(request)
    bot = BOT_INDEX.get(bot_key)
    if not bot:
        raise HTTPException(status_code=404, detail="Unknown bot key")

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
        guild_hints=guild_hints,
    )
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
    guild_id = int(control_state["guild_id"])
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
                placements.append((guild_id, int(channel_id)))

        name_map = await discord_service.resolve_guild_channel_names(token, placements)
        guild_meta = name_map.get((guild_id, None), {})
        session["guild_name"] = guild_meta.get("guild_name") or guild.get("name") or f"Guild {guild_id}"

        if session.get("channel_id"):
            session["channel_name"] = (name_map.get((guild_id, int(session["channel_id"]))) or {}).get("channel_name")
        if session.get("home_channel_id"):
            session["home_channel_name"] = (name_map.get((guild_id, int(session["home_channel_id"]))) or {}).get("channel_name")
        if session.get("feedback_channel_id"):
            session["feedback_channel_name"] = (name_map.get((guild_id, int(session["feedback_channel_id"]))) or {}).get("channel_name")

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
async def bot_control_state(request: Request, bot_key: str, guild_id: int):
    require_api_auth(request)
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
async def guild_control_matrix(request: Request, guild_id: int):
    require_api_auth(request)

    async def collect(bot) -> dict[str, Any]:
        try:
            state = await db.get_bot_control_state(bot.key, guild_id)
            return await _enrich_control_state_with_discord(state)
        except Exception as exc:
            action_logger.warning("Failed control matrix snapshot bot=%s guild=%s: %s", bot.key, guild_id, exc)
            return _control_state_error_payload(bot.key, bot.display_name, guild_id, str(exc))

    bots = await asyncio.gather(*(collect(bot) for bot in MUSIC_BOTS))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "guild_id": str(guild_id),
        "bots": bots,
    }


@app.get("/api/databases")
async def databases(request: Request, include_tables: bool = False):
    require_api_auth(request)
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
    require_api_auth(request)
    try:
        return {"schema": schema, "tables": await db.list_tables(schema)}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}")


@app.post("/api/database/truncate-table")
async def truncate_table(request: Request, payload: TruncateTableRequest):
    require_api_auth(request)
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
    require_api_auth(request)
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
    require_api_auth(request)
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
    guild_id: str | int
    action: str
    payload: Any | None = None

@app.post("/api/bots/control")
async def api_bot_control(request: Request, req: BotControlRequest):
    require_api_auth(request)
    try:
        action, guild_id, payload = await _normalize_bot_control_request(req)
        result = await db.control_bot(req.bot_key, guild_id, action, payload)
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
    except Exception as e:
        await push_feed_event("error", "Bot Control Failed", str(e), source="api")
        raise HTTPException(status_code=500, detail=str(e))

active_connections=[]
command_queue=asyncio.Queue()
bot_health={}
recent_feed_events: deque[dict[str, str]] = deque(maxlen=100)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)

async def broadcast(data: dict):
    dead=[]
    for ws in active_connections:
        try:
            await ws.send_text(json.dumps(data))
        except:
            dead.append(ws)
    for ws in dead:
        active_connections.remove(ws)


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


@app.get("/api/events")
async def list_events(request: Request, limit: int = 50):
    require_api_auth(request)
    bounded_limit = max(1, min(int(limit), 100))
    events = list(recent_feed_events)[-bounded_limit:]
    return {"events": events}

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
    while True:
        cmd = await command_queue.get()
        try:
            await execute_bot_command(cmd)
        except Exception as e:
            await push_feed_event("error", "Command Error", str(e), source="worker")
        command_queue.task_done()

async def monitor_bots():
    while True:
        now=time.time()
        for bot,data in bot_health.items():
            if now-data.get("last_seen",0)>15:
                print(f"[AUTO-HEAL] Restarting {bot}")
        await asyncio.sleep(5)

def verify_token(token: str = Header(None)):
    if token != os.getenv("PANEL_API_KEY"):
        raise HTTPException(403,"Unauthorized")

VALID_ACTIONS={"PAUSE","RESUME","SKIP","STOP","CLEAR","SHUFFLE","LOOP","PLAY","RESTART","FILTER","LEAVE","SET_HOME"}
