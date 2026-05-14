import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .bots import ALL_BOTS

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
load_dotenv(Path(__file__).resolve().parents[2] / "Music" / ".env", override=False)


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip().rstrip("/") for item in raw.split(",") if item.strip()]


def _env_int_set(name: str) -> set[int]:
    raw = os.getenv(name, "")
    values: set[int] = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            values.add(int(item))
        except ValueError:
            continue
    return values


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
    cors_allowed_origins: list[str]
    trusted_hosts: list[str]
    api_token_ttl_seconds: int
    pages_public_url: str
    image_gallery_schema: str
    site_owner_email: str
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_from_email: str
    smtp_use_tls: bool
    shared_music_env_file: str
    session_https_only: bool
    telegram_bot_token: str
    telegram_allowed_chat_ids: set[int]
    telegram_polling_enabled: bool


def load_settings() -> Settings:
    tokens = {bot.key: _env(bot.token_env) for bot in ALL_BOTS}
    session_secret = _env("PANEL_SESSION_SECRET")

    settings = Settings(
        db_host=_env("PANEL_DB_HOST") or _env("DB_HOST") or _env("MYSQL_HOST") or "host.docker.internal",
        db_port=int(_env("PANEL_DB_PORT", "3306")),
        db_user=_env("PANEL_DB_USER") or _env("DB_USER") or _env("MYSQL_USER") or "botuser",
        db_password=_env("PANEL_DB_PASSWORD") or _env("DB_PASSWORD") or _env("MYSQL_PASSWORD") or _env("GWS_DB_PASSWORD"),
        db_default_schema=_env("PANEL_DB_DEFAULT_SCHEMA", "discord_music_gws"),
        admin_username=_env("PANEL_ADMIN_USERNAME", "admin"),
        admin_password=_env("PANEL_ADMIN_PASSWORD"),
        session_secret=session_secret,
        bot_tokens=tokens,
        cors_allowed_origins=_env_csv("PANEL_CORS_ALLOWED_ORIGINS"),
        trusted_hosts=_env_csv("PANEL_TRUSTED_HOSTS"),
        api_token_ttl_seconds=int(_env("PANEL_API_TOKEN_TTL_SECONDS", "43200")),
        pages_public_url=_env("PANEL_PAGES_PUBLIC_URL", "https://heavenlyxenusvr.github.io/SwarmPanel/"),
        image_gallery_schema=_env("IMAGE_GALLERY_DB_SCHEMA") or _env("GALLERY_DB_SCHEMA", "image_gallery"),
        site_owner_email=(_env("SWARM_PANEL_SITE_OWNER_EMAIL") or _env("IMAGE_GALLERY_OWNER_EMAIL") or "heavenlyxenusvr@icloud.com").lower(),
        smtp_host=_env("PANEL_SMTP_HOST") or _env("SMTP_HOST"),
        smtp_port=int(_env("PANEL_SMTP_PORT") or _env("SMTP_PORT") or "587"),
        smtp_username=_env("PANEL_SMTP_USERNAME") or _env("SMTP_USERNAME"),
        smtp_password=_env("PANEL_SMTP_PASSWORD") or _env("SMTP_PASSWORD"),
        smtp_from_email=_env("PANEL_SMTP_FROM_EMAIL") or _env("SMTP_FROM_EMAIL") or _env("PANEL_SMTP_USERNAME") or _env("SMTP_USERNAME"),
        smtp_use_tls=(_env("PANEL_SMTP_USE_TLS") or _env("SMTP_USE_TLS") or "true").lower() not in {"0", "false", "no", "off"},
        shared_music_env_file=_env("PANEL_SHARED_MUSIC_ENV_FILE", str(Path(__file__).resolve().parents[2] / "Music" / ".env")),
        session_https_only=_env_bool("PANEL_SESSION_HTTPS_ONLY", False),
        telegram_bot_token=_env("PANEL_TELEGRAM_BOT_TOKEN") or _env("TELEGRAM_BOT_TOKEN"),
        telegram_allowed_chat_ids=_env_int_set("PANEL_TELEGRAM_ALLOWED_CHAT_IDS") or _env_int_set("TELEGRAM_ALLOWED_CHAT_IDS"),
        telegram_polling_enabled=_env_bool("PANEL_TELEGRAM_POLLING_ENABLED", bool(_env("PANEL_TELEGRAM_BOT_TOKEN") or _env("TELEGRAM_BOT_TOKEN"))),
    )
    validate_settings(settings)
    return settings


def validate_settings(settings: Settings) -> None:
    missing = []
    if not settings.db_password:
        missing.append("PANEL_DB_PASSWORD")
    if not settings.admin_password:
        missing.append("PANEL_ADMIN_PASSWORD")
    if not settings.session_secret:
        missing.append("PANEL_SESSION_SECRET")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
