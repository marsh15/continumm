from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text
from sqlalchemy.engine import make_url

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

MIGRATION_SCHEMA = "continuum_migrations"

# Migrations use an operator-controlled connection. The ingress credential never
# has DDL privileges and is never used by this environment.
database_url = os.environ.get("CONTINUUM_MIGRATION_DATABASE_URL")
if not database_url:
    raise RuntimeError("migration_database_url_required")

url = make_url(database_url)
if url.drivername in {"postgres", "postgresql"}:
    url = url.set(drivername="postgresql+psycopg")
config.set_main_option(
    "sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%")
)


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        hide_parameters=True,
        connect_args={
            "connect_timeout": 5,
            "options": "-c statement_timeout=15000 -c lock_timeout=3000",
        },
    )
    try:
        with connectable.connect() as connection:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {MIGRATION_SCHEMA}"))
            connection.execute(text(f"REVOKE ALL ON SCHEMA {MIGRATION_SCHEMA} FROM PUBLIC"))
            connection.commit()
            context.configure(
                connection=connection,
                target_metadata=None,
                compare_type=True,
                transaction_per_migration=True,
                version_table_schema=MIGRATION_SCHEMA,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    raise RuntimeError("offline_migrations_unsupported")
run_migrations_online()
