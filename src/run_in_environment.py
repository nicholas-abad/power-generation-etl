"""Run an ETL or psql command against an explicitly selected Neon environment.

Local credentials come only from .env.<environment>.<role>; --from-process
uses a complete GitHub environment instead. Never falls back to .env.
Without a command, checks the connection and prints its non-secret identity.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import quote, urlencode

from dotenv import dotenv_values
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = (
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_SSLMODE",
)


def build_environment(environment, role, values, inherited, config):
    """Reject incomplete, mixed, or incorrectly targeted credentials."""
    missing = [key for key in REQUIRED if not values.get(key)]
    if missing:
        raise ValueError("Missing connection settings: " + ", ".join(missing))
    target = config["environments"][environment]
    endpoint = target["endpoint_id"]
    other_endpoints = {
        item["endpoint_id"]
        for name, item in config["environments"].items()
        if name != environment
    }
    if endpoint in other_endpoints:
        raise ValueError("Environment endpoints must be distinct")
    allowed_hosts = {
        f"{endpoint}.{config['proxy_host']}",
        f"{endpoint}-pooler.{config['proxy_host']}",
    }
    if values["POSTGRES_HOST"] not in allowed_hosts:
        raise ValueError(
            f"Connection host does not match {environment}'s pinned endpoint"
        )
    if values["POSTGRES_USER"] != config["roles"][role]:
        raise ValueError(f"Connection user does not match the requested {role} role")
    if values["POSTGRES_DB"] != config["database"]:
        raise ValueError("Connection database does not match the configured database")
    if values["POSTGRES_PORT"] != "5432":
        raise ValueError("Neon connections require port 5432")
    if values["POSTGRES_SSLMODE"] not in {"require", "verify-full"}:
        raise ValueError("Neon connections must use TLS")

    # Avoid inherited libpq service files, hostaddr, URL, or production fields
    # steering psql/Python differently. All supported connection forms agree.
    child = {
        key: value
        for key, value in inherited.items()
        if not key.startswith(("POSTGRES_", "PG"))
        and key not in {"DATABASE_URL", "DIRECT_DATABASE_URL"}
    }
    child.update({key: values[key] for key in REQUIRED})
    for suffix, pg_key in {
        "HOST": "PGHOST",
        "PORT": "PGPORT",
        "DB": "PGDATABASE",
        "USER": "PGUSER",
        "PASSWORD": "PGPASSWORD",
        "SSLMODE": "PGSSLMODE",
    }.items():
        child[pg_key] = values[f"POSTGRES_{suffix}"]
    user = quote(values["POSTGRES_USER"], safe="")
    password = quote(values["POSTGRES_PASSWORD"], safe="")
    host = values["POSTGRES_HOST"]
    database = quote(values["POSTGRES_DB"], safe="")
    query = urlencode({"sslmode": values["POSTGRES_SSLMODE"]})
    child["DATABASE_URL"] = (
        f"postgresql://{user}:{password}@{host}:5432/{database}?{query}"
    )
    direct_host = f"{endpoint}.{config['proxy_host']}"
    child["DIRECT_DATABASE_URL"] = (
        f"postgresql://{user}:{password}@{direct_host}:5432/{database}?{query}"
    )
    child["ETL_ENVIRONMENT"] = environment
    # Children import modules that call load_dotenv(). Do not let an unrelated
    # .env restore libpq routing settings after this environment was checked.
    child["PYTHON_DOTENV_DISABLED"] = "1"
    child["NEON_BRANCH_ID"] = target["branch_id"]
    child["NEON_ENDPOINT_ID"] = endpoint
    child["PGCONNECT_TIMEOUT"] = "15"
    child["PGOPTIONS"] = "-c timezone=UTC"
    return child


def check_connection(child):
    """Read-only probe before handing credentials to the requested command."""
    # libpq also reads this process's environment. PGHOSTADDR/PGSERVICE must
    # not steer the probe before the command receives the sanitized child env.
    inherited_pg = {
        key: value for key, value in os.environ.items() if key.startswith("PG")
    }
    try:
        for key in inherited_pg:
            del os.environ[key]
        with psycopg2.connect(child["DATABASE_URL"], connect_timeout=15) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_user")
                if cursor.fetchone() != (child["POSTGRES_DB"], child["POSTGRES_USER"]):
                    raise ValueError(
                        "Connected database identity does not match configuration"
                    )
    finally:
        os.environ.update(inherited_pg)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--environment", required=True, choices=("staging", "production")
    )
    parser.add_argument(
        "--role", choices=("writer", "reader", "owner"), default="writer"
    )
    parser.add_argument("--from-process", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        config = json.loads((ROOT / "config/environments.json").read_text())
        if args.from_process:
            values = dict(os.environ)
        else:
            path = ROOT / f".env.{args.environment}.{args.role}"
            if not path.is_file():
                raise ValueError(f"Missing credential file: {path.name}")
            values = dotenv_values(path, interpolate=False)
        child = build_environment(
            args.environment, args.role, values, os.environ, config
        )
        check_connection(child)
    except (ValueError, OSError, psycopg2.Error) as error:
        # Connection exceptions can include a DSN; don't echo them or secrets.
        message = str(error) if isinstance(error, ValueError) else type(error).__name__
        print(f"Environment check failed: {message}", file=sys.stderr)
        return 1
    print(
        f"Target: {args.environment} / {child['NEON_BRANCH_ID']} / "
        f"{child['POSTGRES_DB']} / {child['POSTGRES_USER']}",
        flush=True,
    )
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    return subprocess.run(command, env=child).returncode if command else 0


if __name__ == "__main__":
    sys.exit(main())
