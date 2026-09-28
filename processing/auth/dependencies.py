import jwt
from fastapi import Cookie, Depends, HTTPException, status

from processing.auth.security import decode_access_token
from processing.auth.service import find_user_by_id


async def get_current_user(logflow_session: str | None = Cookie(default=None)) -> dict:
    if not logflow_session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")

    try:
        user_id = decode_access_token(logflow_session)
    except (ValueError, TypeError, jwt.InvalidTokenError, RuntimeError):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")

    user = find_user_by_id(user_id)
    if not user or not user["is_active"]:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return user
