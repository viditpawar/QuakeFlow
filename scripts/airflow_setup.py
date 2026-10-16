"""One-time setup for Airflow: generate its secrets and create its database in PostgreSQL.

    python scripts/airflow_setup.py

Creates, in the git-ignored folder secrets/:
    airflow.env              the database connection string and the keys Airflow needs
    airflow_passwords.json   the login for the Airflow UI (user "quakeflow")

and, in the running PostgreSQL container, a dedicated role and database called "airflow". Airflow
therefore never uses the lakehouse catalog's credentials. Running it again keeps the existing secrets
and only makes sure the database matches them. Use --rotate to generate new ones.
"""

import argparse
import base64
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECRETS = ROOT / "secrets"
ENV_FILE = SECRETS / "airflow.env"
PASSWORDS_FILE = SECRETS / "airflow_passwords.json"
UI_USER = "quakeflow"
DB_NAME = DB_ROLE = "airflow"
POSTGRES_CONTAINER = "quakeflow-postgres"


def new_secrets() -> dict[str, str]:
    """Fresh random values. The Fernet key is 32 random bytes, URL-safe base64 encoded, as Airflow expects."""
    return {
        "db_password": secrets.token_urlsafe(24),
        "fernet_key": base64.urlsafe_b64encode(os.urandom(32)).decode(),
        "jwt_secret": secrets.token_urlsafe(48),
        "api_secret_key": secrets.token_urlsafe(48),
        "ui_password": secrets.token_urlsafe(18),
    }


def render_env(values: dict[str, str]) -> str:
    """Contents of secrets/airflow.env. Compose reads it for the Airflow containers only."""
    return (
        f"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://{DB_ROLE}:{values['db_password']}@postgres:5432/{DB_NAME}\n"
        f"AIRFLOW__CORE__FERNET_KEY={values['fernet_key']}\n"
        f"AIRFLOW__API_AUTH__JWT_SECRET={values['jwt_secret']}\n"
        f"AIRFLOW__API__SECRET_KEY={values['api_secret_key']}\n"
        f"AIRFLOW_DB_PASSWORD={values['db_password']}\n"
    )


def read_existing() -> dict[str, str] | None:
    """The secrets from a previous run, or None if there are none or the files are incomplete."""
    if not (ENV_FILE.exists() and PASSWORDS_FILE.exists()):
        return None
    pairs = dict(line.split("=", 1) for line in ENV_FILE.read_text(encoding="utf-8").splitlines() if "=" in line)
    try:
        return {
            "db_password": pairs["AIRFLOW_DB_PASSWORD"],
            "fernet_key": pairs["AIRFLOW__CORE__FERNET_KEY"],
            "jwt_secret": pairs["AIRFLOW__API_AUTH__JWT_SECRET"],
            "api_secret_key": pairs["AIRFLOW__API__SECRET_KEY"],
            "ui_password": json.loads(PASSWORDS_FILE.read_text(encoding="utf-8"))[UI_USER],
        }
    except (KeyError, ValueError):
        return None


def database_sql(db_password: str) -> str:
    """SQL that creates or updates the Airflow role and database. Safe to run repeatedly.

    The password only ever contains letters, digits, '-' and '_' (token_urlsafe), so it is safe inside
    a quoted SQL string, and it is refused here if that ever stops being true."""
    if not all(c.isalnum() or c in "-_" for c in db_password):
        raise ValueError("database password contains characters that are not safe to put in SQL")
    return (
        f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{DB_ROLE}') "
        f"THEN CREATE ROLE {DB_ROLE} LOGIN; END IF; END $$;\n"
        f"ALTER ROLE {DB_ROLE} WITH LOGIN PASSWORD '{db_password}';\n"
        f"SELECT 'CREATE DATABASE {DB_NAME} OWNER {DB_ROLE}' "
        f"WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{DB_NAME}')\\gexec\n"
    )


def apply_to_postgres(sql: str) -> None:
    """Run the SQL in the PostgreSQL container, passing it on stdin so the password is not in the command line."""
    user = os.getenv("POSTGRES_USER", "quakeflow")
    result = subprocess.run(
        ["docker", "exec", "-i", POSTGRES_CONTAINER, "psql", "-U", user, "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-q"],
        input=sql, capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        raise SystemExit(f"could not configure PostgreSQL: {result.stderr.strip() or result.stdout.strip()}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rotate", action="store_true", help="generate new secrets even if some exist")
    args = parser.parse_args(argv)

    values = None if args.rotate else read_existing()
    created = values is None
    if created:
        values = new_secrets()
        SECRETS.mkdir(exist_ok=True)
        ENV_FILE.write_text(render_env(values), encoding="utf-8")
        PASSWORDS_FILE.write_text(json.dumps({UI_USER: values["ui_password"]}), encoding="utf-8")
    apply_to_postgres(database_sql(values["db_password"]))

    print(("Created" if created else "Kept") + f" the secrets in {SECRETS.relative_to(ROOT)}/ (not committed).")
    print(f"PostgreSQL role and database '{DB_NAME}' are ready.")
    print(f"Airflow UI: http://127.0.0.1:8080   user: {UI_USER}   password: in secrets/airflow_passwords.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
