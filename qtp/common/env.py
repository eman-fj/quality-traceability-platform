"""Host-side connection settings for the SDK scripts (read from .env).

The SDK scripts (traceability_checker, shift_quality_report) run on the host, so
they reach the dockerised services on the remapped HOST ports from .env — not the
in-network ports the pipelines use. Nothing is hardcoded; values come from .env
with the compose defaults as fallback.
"""

import os

from dotenv import load_dotenv


def _env() -> os._Environ:
    load_dotenv()
    return os.environ


def pg_conn_params() -> dict:
    """psycopg2 connection kwargs for the quality warehouse (host side)."""
    e = _env()
    return {
        "host": e.get("PG_HOST", "localhost"),
        "port": int(e.get("POSTGRES_PORT", "5432")),
        "dbname": e.get("POSTGRES_DB", "quality"),
        "user": e.get("POSTGRES_USER", "qtp"),
        "password": e.get("POSTGRES_PASSWORD", "qtp_pw"),
    }


def wiremock_base_url() -> str:
    """Base URL for the WireMock-stubbed external APIs (alerts, Jira), host side."""
    e = _env()
    return e.get("WIREMOCK_BASE_URL", "http://localhost:{}".format(e.get("WIREMOCK_PORT", "8080")))


def mysql_conn_params() -> dict:
    """pymysql connection kwargs for the ERP system (host side)."""
    e = _env()
    return {
        "host": e.get("MYSQL_HOST", "localhost"),
        "port": int(e.get("MYSQL_PORT", "3306")),
        "database": e.get("MYSQL_DB", "erp"),
        "user": e.get("MYSQL_USER", "qtp"),
        "password": e.get("MYSQL_PASSWORD", "qtp_pw"),
    }


def minio_settings() -> dict:
    """boto3/S3 settings for MinIO object storage (host side, TLS endpoint)."""
    e = _env()
    return {
        "endpoint_url": e.get("MINIO_S3_ENDPOINT",
                              "https://localhost:{}".format(e.get("MINIO_API_PORT", "9000"))),
        "access_key": e.get("MINIO_ROOT_USER", "minioadmin"),
        "secret_key": e.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    }


def mailhog_smtp() -> tuple[str, int]:
    """(host, port) of the Mailhog SMTP sink (host side)."""
    e = _env()
    return (e.get("MAILHOG_HOST", "localhost"), int(e.get("MAILHOG_SMTP_PORT", "1025")))
