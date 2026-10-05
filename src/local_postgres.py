"""Shared connection guards for disposable local tests and rehearsals."""

import os
import re

from psycopg2.extensions import parse_dsn


def local_settings(dsn: str, database_pattern: str) -> dict[str, str]:
    """Validate explicit local credentials and disable implicit routing sources.

    This modifies the process environment before either psycopg2 or SQLAlchemy
    can connect. Test fixtures should restore that environment when they finish.
    """
    parts = parse_dsn(dsn)
    if set(parts) - {"host", "port", "dbname", "user", "password"}:
        raise ValueError("Only explicit local DSN settings are allowed")
    if parts.get("host") == "localhost":
        parts["host"] = "127.0.0.1"
    if parts.get("host") not in {
        "127.0.0.1",
        "/tmp",
        "/private/tmp",
        "/tmp/coal-hotfix-ci",
    }:
        raise ValueError("Only local PostgreSQL is allowed")
    if not re.fullmatch(database_pattern, parts.get("dbname", "")):
        raise ValueError("Use a disposable database in the allowed namespace")
    port = parts.get("port", "5432")
    if (
        parts.get("user") != "postgres"
        or not port.isdigit()
        or not 1 <= int(port) <= 65535
    ):
        raise ValueError("Use the disposable local postgres owner and a valid port")
    for key in list(os.environ):
        if key.startswith(("PG", "POSTGRES_")) or key in {
            "DATABASE_URL",
            "DIRECT_DATABASE_URL",
        }:
            del os.environ[key]
    os.environ["PYTHON_DOTENV_DISABLED"] = "1"
    return parts


def verify_local_connection(connection, database: str) -> None:
    """Verify the actual server and database before any destructive SQL."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database(), inet_server_addr()::text")
        name, address = cursor.fetchone()
    if name != database or address not in {None, "127.0.0.1"}:
        raise ValueError("Connection is not the expected local database")
