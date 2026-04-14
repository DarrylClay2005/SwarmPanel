import os
import secrets
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .bots import ALL_BOTS

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_default_schema: str
    admin_username: str
    admin_password: str
    session_secret: str
    bot_tokens: dict[str, str]


def load_settings() -> Settings:
    tokens = {bot.key: _env(bot.token_env) for bot in ALL_BOTS}
    session_secret = _env("PANEL_SESSION_SECRET")
    if not session_secret:
        session_secret = secrets.token_urlsafe(48)

    settings = Settings(
        db_host=_env("PANEL_DB_HOST", "127.0.0.1"),
        db_port=int(_env("PANEL_DB_PORT", "3306")),
        db_user=_env("PANEL_DB_USER", "botuser"),
        db_password=_env("PANEL_DB_PASSWORD"),
        db_default_schema=_env("PANEL_DB_DEFAULT_SCHEMA", "discord_music_gws"),
        admin_username=_env("PANEL_ADMIN_USERNAME", "admin"),
        admin_password=_env("PANEL_ADMIN_PASSWORD"),
        session_secret=session_secret,
        bot_tokens=tokens,
    )
    validate_settings(settings)
    return settings


def validate_settings(settings: Settings) -> None:
    missing = []
    if not settings.db_password:
        missing.append("PANEL_DB_PASSWORD")
    if not settings.admin_password:
        missing.append("PANEL_ADMIN_PASSWORD")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
