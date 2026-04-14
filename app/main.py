import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

from .auth import SESSION_AUTH_KEY, is_authenticated, require_api_auth, verify_credentials
from .bots import ALL_BOTS, BOT_INDEX
from .config import load_settings
from .database import PanelDatabase
from .discord_api import DiscordInventoryService


BASE_DIR = Path(__file__).resolve().parent
settings = load_settings()
db = PanelDatabase(settings)
discord_service = DiscordInventoryService()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
action_logger = logging.getLogger("swarm_panel.actions")


class TruncateTableRequest(BaseModel):
    schema_name: str
    table_name: str
    confirm_text: str


class TruncateSchemaRequest(BaseModel):
    schema_name: str
    confirm_text: str


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await db.connect()
    except Exception as exc:
        logging.getLogger("swarm_panel").warning("Initial DB connection failed at startup: %s", exc)
    await discord_service.connect()
    yield
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
        placements = [(int(s["guild_id"]), int(s["channel_id"]) if s.get("channel_id") else None) for s in sessions]
        try:
            name_map = await discord_service.resolve_guild_channel_names(token, placements)
            for session in sessions:
                key = (int(session["guild_id"]), int(session["channel_id"]) if session.get("channel_id") else None)
                names = name_map.get(key)
                if names:
                    session["guild_name"] = names.get("guild_name")
                    session["channel_name"] = names.get("channel_name")
        except Exception as exc:
            bot["discord"]["name_resolution_error"] = str(exc)

    return data


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
    payload: str | None = None

@app.post("/api/bots/control")
async def api_bot_control(request: Request, req: BotControlRequest):
    require_api_auth(request)
    try:
        await db.control_bot(req.bot_key, req.guild_id, req.action, req.payload)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- WEBSOCKET + QUEUE + HEALTH ---
from fastapi import WebSocket, WebSocketDisconnect, Header, HTTPException
import asyncio, time, json, os

active_connections=[]
command_queue=asyncio.Queue()
bot_health={}

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

async def command_worker():
    while True:
        cmd = await command_queue.get()
        try:
            await execute_bot_command(cmd)
        except Exception as e:
            await broadcast({"type":"error","data":{"title":"Command Error","description":str(e),"timestamp":str(time.time())}})
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

VALID_ACTIONS={"PAUSE","RESUME","SKIP","STOP","CLEAR","SHUFFLE","LOOP","RESTART"}

@app.on_event("startup")
async def startup_tasks():
    asyncio.create_task(command_worker())
    asyncio.create_task(monitor_bots())
