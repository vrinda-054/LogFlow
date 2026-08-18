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
import logging
from contextlib import contextmanager

import psycopg2
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level singleton for the SQLAlchemy engine.
# Initialised lazily on first call to get_engine().
# ---------------------------------------------------------------------------
_engine = None

# ---------------------------------------------------------------------------
# The LogFlow schema name used in processing/db/schema.sql.
# All tables live under the 'logflow' schema.
# ---------------------------------------------------------------------------
LOGFLOW_SCHEMA = "logflow"


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

    The connection has autocommit=False, so the caller is responsible for
    calling conn.commit() or conn.rollback() and conn.close().

    The search_path is automatically set to the 'logflow' schema so that
    queries can reference tables without the schema prefix (e.g.,
    ``processed_logs`` instead of ``logflow.processed_logs``).

    Returns
    -------
    psycopg2.extensions.connection
        A raw database connection with autocommit=False.

    Raises
    ------
    EnvironmentError
        If DATABASE_URL is not set.
    psycopg2.OperationalError
        If the database is unreachable.
    """
    url = _get_database_url()
    conn = psycopg2.connect(url)
    conn.autocommit = False

    # Set the search_path so callers can use unqualified table names.
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {LOGFLOW_SCHEMA}, public;")
    conn.commit()

    logger.debug("psycopg2 connection opened (search_path=%s)", LOGFLOW_SCHEMA)
    return conn


def get_engine():
    """
    Return a SQLAlchemy engine (singleton, thread-safe).

    The engine uses a connection pool (pool_size=5, max_overflow=10) and
    ``pool_pre_ping=True`` to automatically discard stale connections.

    Every new connection in the pool automatically has its ``search_path``
    set to the ``logflow`` schema via a SQLAlchemy ``connect`` event.

    Returns
    -------
    sqlalchemy.engine.Engine

    Notes
    -----
    Thread-safe. The engine is created once and reused across all callers.
    """
    global _engine
    if _engine is None:
        url = _get_database_url()
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )

        # Automatically set search_path for every new raw DBAPI connection.
        @event.listens_for(_engine, "connect")
        def _set_search_path(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute(f"SET search_path TO {LOGFLOW_SCHEMA}, public;")
            cursor.close()
            dbapi_connection.commit()

        logger.info("SQLAlchemy engine created (pool_size=5)")

    return _engine


@contextmanager
def get_session():
    """
    Yield a SQLAlchemy Session as a context manager.

    Usage
    -----
    with get_session() as session:
        results = session.execute(text("SELECT ..."))

    On clean exit : session.commit()
    On exception  : session.rollback()
    Always        : session.close()

    Yields
    ------
    sqlalchemy.orm.Session
    """
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
