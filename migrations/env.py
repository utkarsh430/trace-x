"""Alembic environment.

The database URL is assembled from environment variables and never committed.
Migrations run as the owning role (the only one that may create schemas, roles
and grants); the application role deliberately cannot.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# No declarative models yet -- Phase 1 introduces them. Autogenerate is not used
# for the security-critical grants, which must be written explicitly.
target_metadata = None

SCHEMAS = ("app", "audit", "groundtruth", "eval", "external")


def database_url() -> str:
    if url := os.getenv("TRACE_DATABASE_URL"):
        return url
    user = os.getenv("POSTGRES_SUPERUSER", "tracex_owner")
    password = os.getenv("POSTGRES_SUPERUSER_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5442")
    db = os.getenv("POSTGRES_DB", "tracex")
    if not password:
        raise RuntimeError(
            "POSTGRES_SUPERUSER_PASSWORD is not set. Run `cp .env.example .env` "
            "(or `make setup`) and export it, or set TRACE_DATABASE_URL."
        )
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"


def run_migrations_offline() -> None:
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema="app",
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = database_url()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)

    with connectable.connect() as connection:
        # The version table lives in `app`, which migration 0001 creates. Ensure
        # it exists before Alembic tries to write its bookkeeping row.
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS app")
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema="app",
            include_schemas=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
