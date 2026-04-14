import hmac

from fastapi import HTTPException, Request


SESSION_AUTH_KEY = "swarm_panel_authenticated"


def is_authenticated(request: Request) -> bool:
    return bool(request.session.get(SESSION_AUTH_KEY))


def require_api_auth(request: Request) -> None:
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Authentication required")


def verify_credentials(username: str, password: str, expected_username: str, expected_password: str) -> bool:
    return hmac.compare_digest(username, expected_username) and hmac.compare_digest(password, expected_password)
