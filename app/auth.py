import hmac

from fastapi import HTTPException, Request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer


SESSION_AUTH_KEY = "swarm_panel_authenticated"
API_TOKEN_SALT = "swarm_panel_api_token"


def is_authenticated(request: Request) -> bool:
    return bool(request.session.get(SESSION_AUTH_KEY))


def extract_bearer_token(authorization_header: str | None) -> str | None:
    header = str(authorization_header or "").strip()
    if not header.lower().startswith("bearer "):
        return None
    token = header[7:].strip()
    return token or None


def issue_api_token(secret_key: str, username: str) -> str:
    serializer = URLSafeTimedSerializer(secret_key, salt=API_TOKEN_SALT)
    return serializer.dumps({"username": username})


def verify_api_token(token: str | None, secret_key: str, max_age_seconds: int) -> dict | None:
    if not token:
        return None
    serializer = URLSafeTimedSerializer(secret_key, salt=API_TOKEN_SALT)
    try:
        data = serializer.loads(token, max_age=max_age_seconds)
    except (BadSignature, SignatureExpired):
        return None
    if isinstance(data, dict):
        return data
    return {"username": str(data)}


def get_api_auth(
    request: Request,
    *,
    secret_key: str,
    max_age_seconds: int,
) -> dict | None:
    if is_authenticated(request):
        return {"mode": "session"}

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
