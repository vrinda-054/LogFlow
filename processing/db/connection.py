"""
processing/db/connection.py — Person 3 (Processing Layer)
===========================================================

Role
----
Provides a single, shared PostgreSQL connection factory for all modules in
the processing/ layer. Both aggregator.py and api/main.py use this module
to obtain database connections/sessions. Centralising the factory here means
connection string changes only happen in one place.

Input
-----
  Environment variable: DATABASE_URL
    Format: postgresql://user:password@host:port/dbname
    Docker internal: postgresql://logflow_user:changeme@postgres:5432/logflow
    Local dev:       postgresql://logflow_user:changeme@localhost:5432/logflow
    See .env.example for all variable names.

Output / Interface
------------------
  get_connection() → psycopg2.connection
    Returns a raw psycopg2 connection (autocommit=False).
    Caller is responsible for commit/rollback/close.

  get_engine() → sqlalchemy.Engine
    Returns a SQLAlchemy engine (for use with ORM or pandas if preferred).

  get_session() → sqlalchemy.orm.Session (context manager)
    Yields a SQLAlchemy Session. Commits on clean exit, rolls back on exception.

Usage in aggregator.py
-----------------------
  from processing.db.connection import get_connection
  conn = get_connection()
  try:
      with conn.cursor() as cur:
          cur.execute("INSERT INTO logflow.processed_logs ...")
      conn.commit()
  finally:
      conn.close()

Usage in api/main.py (FastAPI dependency)
------------------------------------------
  from processing.db.connection import get_session
  @app.get("/metrics/throughput")
  def get_throughput(db=Depends(get_session)):
      ...

Consumed by
-----------
  processing/aggregator.py
  processing/api/main.py
"""

import os
from contextlib import contextmanager

# ---------------------------------------------------------------------------
# TODO (Person 3): uncomment and install psycopg2-binary, sqlalchemy
# ---------------------------------------------------------------------------
# import psycopg2
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker, Session


def _get_database_url() -> str:
    """
    Read and return DATABASE_URL from the environment.

    Raises
    ------
    EnvironmentError
        If DATABASE_URL is not set.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise EnvironmentError(
            "DATABASE_URL is not set. Copy .env.example → .env and fill in values."
        )
    return url


def get_connection():
    """
    Open and return a new psycopg2 database connection.

    Returns
    -------
    psycopg2.extensions.connection
        A raw database connection with autocommit=False.
        The caller must call conn.commit() or conn.rollback() and conn.close().

    Raises
    ------
    EnvironmentError
        If DATABASE_URL is not set.
    psycopg2.OperationalError
        If the database is unreachable.
    """
    # TODO: return psycopg2.connect(_get_database_url())
    raise NotImplementedError("get_connection: install psycopg2-binary and implement")


def get_engine():
    """
    Return a SQLAlchemy engine (singleton, thread-safe).

    Returns
    -------
    sqlalchemy.engine.Engine

    Notes
    -----
    Uses a connection pool (default pool_size=5). Safe for concurrent FastAPI
    request handlers.
    """
    # TODO: return create_engine(_get_database_url(), pool_pre_ping=True)
    raise NotImplementedError("get_engine: install sqlalchemy and implement")


@contextmanager
def get_session():
    """
    Yield a SQLAlchemy Session as a context manager.

    Usage
    -----
    with get_session() as session:
        results = session.execute(...)

    On clean exit: session.commit()
    On exception : session.rollback()
    Always       : session.close()
    """
    # TODO:
    # SessionLocal = sessionmaker(bind=get_engine())
    # session = SessionLocal()
    # try:
    #     yield session
    #     session.commit()
    # except Exception:
    #     session.rollback()
    #     raise
    # finally:
    #     session.close()
    raise NotImplementedError("get_session: install sqlalchemy and implement")
