import hmac

from fastapi import HTTPException, Request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer


SESSION_AUTH_KEY = "swarm_panel_authenticated"
SESSION_USERNAME_KEY = "swarm_panel_username"
SESSION_ROLE_KEY = "swarm_panel_role"
SESSION_GUILD_ID_KEY = "swarm_panel_guild_id"
SESSION_ADMIN_MODE_KEY = "swarm_panel_admin_mode"
API_TOKEN_SALT = "swarm_panel_api_token"


def is_authenticated(request: Request) -> bool:
    return bool(request.session.get(SESSION_AUTH_KEY))


def extract_bearer_token(authorization_header: str | None) -> str | None:
    header = str(authorization_header or "").strip()
    if not header.lower().startswith("bearer "):
        return None
    token = header[7:].strip()
    return token or None


def issue_api_token(
    secret_key: str,
    username: str,
    *,
    role: str = "admin",
    guild_id: str | None = None,
    admin_mode: bool | None = None,
) -> str:
    serializer = URLSafeTimedSerializer(secret_key, salt=API_TOKEN_SALT)
    payload = {"username": username, "role": role}
    if guild_id:
        payload["guild_id"] = str(guild_id)
    if admin_mode is None:
        admin_mode = str(role or "").lower() == "admin" and not guild_id
    payload["admin_mode"] = bool(admin_mode)
    return serializer.dumps(payload)


def verify_api_token(token: str | None, secret_key: str, max_age_seconds: int) -> dict | None:
    if not token:
        return None
    serializer = URLSafeTimedSerializer(secret_key, salt=API_TOKEN_SALT)
    try:
        data = serializer.loads(token, max_age=max_age_seconds)
    except (BadSignature, SignatureExpired):
        return None
    if isinstance(data, dict):
        data.setdefault("role", "account" if data.get("guild_id") else "admin")
        data.setdefault("admin_mode", str(data.get("role") or "").lower() == "admin" and not data.get("guild_id"))
        return data
    return {"username": str(data), "role": "admin", "admin_mode": True}


def get_api_auth(
    request: Request,
    *,
    secret_key: str,
    max_age_seconds: int,
) -> dict | None:
    if is_authenticated(request):
        role = request.session.get(SESSION_ROLE_KEY) or "admin"
        guild_id = request.session.get(SESSION_GUILD_ID_KEY)
        admin_mode = request.session.get(SESSION_ADMIN_MODE_KEY)
        if admin_mode is None:
            admin_mode = str(role or "").lower() == "admin" and not guild_id
        auth = {
            "mode": "session",
            "username": request.session.get(SESSION_USERNAME_KEY),
            "role": role,
            "admin_mode": bool(admin_mode),
        }
        if guild_id:
            auth["guild_id"] = str(guild_id)
        return auth

    token = extract_bearer_token(request.headers.get("authorization"))
    data = verify_api_token(token, secret_key, max_age_seconds)
    if not data:
        return None
    return {"mode": "token", **data}


def require_api_auth(
    request: Request,
    *,
    secret_key: str,
    max_age_seconds: int,
) -> dict:
    auth = get_api_auth(request, secret_key=secret_key, max_age_seconds=max_age_seconds)
    if not auth:
        raise HTTPException(status_code=401, detail="Authentication required")
    return auth


def verify_credentials(username: str, password: str, expected_username: str, expected_password: str) -> bool:
    return hmac.compare_digest(username, expected_username) and hmac.compare_digest(password, expected_password)
