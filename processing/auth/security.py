import os
from datetime import datetime, timedelta, timezone

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError


_password_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    return _password_hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return _password_hasher.verify(password_hash, password)
    except (InvalidHashError, VerificationError, VerifyMismatchError):
        return False


def _secret_key() -> str:
    secret = os.environ.get("JWT_SECRET_KEY")
    if not secret:
        raise RuntimeError("JWT_SECRET_KEY must be configured")
    return secret


def create_access_token(user_id: int, remember_me: bool = False) -> tuple[str, int]:
    configured_minutes = int(os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
    expiration_minutes = configured_minutes * 24 if remember_me else configured_minutes
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=expiration_minutes)
    token = jwt.encode(
        {"sub": str(user_id), "exp": expires_at},
        _secret_key(),
        algorithm=os.environ.get("JWT_ALGORITHM", "HS256"),
    )
    return token, expiration_minutes * 60


def decode_access_token(token: str) -> int:
    payload = jwt.decode(
        token,
        _secret_key(),
        algorithms=[os.environ.get("JWT_ALGORITHM", "HS256")],
    )
    subject = payload.get("sub")
    if not subject:
        raise jwt.InvalidTokenError("Token subject is missing")
    return int(subject)
