from sqlalchemy import text

from processing.auth.security import hash_password, verify_password
from processing.db.connection import get_session


USERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS logflow.users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    email VARCHAR(320) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE
)
"""


def ensure_users_table() -> None:
    with get_session() as session:
        session.execute(text(USERS_TABLE_SQL))


def find_user_by_email(email: str) -> dict | None:
    with get_session() as session:
        row = session.execute(
            text("SELECT id, name, email, password_hash, is_active FROM logflow.users WHERE email = :email"),
            {"email": email},
        ).mappings().first()
        return dict(row) if row else None


def find_user_by_id(user_id: int) -> dict | None:
    with get_session() as session:
        row = session.execute(
            text("SELECT id, name, email, password_hash, is_active FROM logflow.users WHERE id = :id"),
            {"id": user_id},
        ).mappings().first()
        return dict(row) if row else None


def create_user(name: str, email: str, password: str) -> dict:
    with get_session() as session:
        row = session.execute(
            text("""
                INSERT INTO logflow.users (name, email, password_hash)
                VALUES (:name, :email, :password_hash)
                RETURNING id, name, email, is_active
            """),
            {"name": name, "email": email, "password_hash": hash_password(password)},
        ).mappings().one()
        return dict(row)


def authenticate_user(email: str, password: str) -> dict | None:
    user = find_user_by_email(email)
    if not user or not user["is_active"] or not verify_password(password, user["password_hash"]):
        return None
    return user
