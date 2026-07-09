"""Host-side connection settings for the SDK scripts (read from .env).

The SDK scripts (traceability_checker, shift_quality_report) run on the host, so
they reach the dockerised services on the remapped HOST ports from .env - not the
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


def job_runtime_secrets() -> dict:
    """Real alerting secrets (Teams webhook, Jira creds), for JOB creation only.

    These must never be baked into a pipeline's own `pipeline.parameters` (that dict is
    published with the pipeline and lands verbatim in `qtp pipelines export`'s JSON, which
    is committed to the repo under /pipelines - see Master Programme Doc §9.2: "Connection
    credentials are stored as Control Hub runtime parameters and injected at job execution
    time," not baked into the pipeline definition). Instead, `qtp jobs create` passes this
    dict as the JOB's `runtime_parameters` at job-creation time; the published pipeline only
    ever declares these parameter names with an empty placeholder default.
    """
    e = _env()
    return {
        "TEAMS_ALERT_WEBHOOK_URL": e.get("TEAMS_ALERT_WEBHOOK_URL", ""),
        "JIRA_BASE_URL":           e.get("JIRA_BASE_URL", ""),
        "JIRA_USER_EMAIL":         e.get("JIRA_USER_EMAIL", ""),
        "JIRA_API_TOKEN":          e.get("JIRA_API_TOKEN", ""),
        "JIRA_PROJECT_KEY":        e.get("JIRA_PROJECT_KEY", "QUAL"),
        "JIRA_ISSUE_TYPE":         e.get("JIRA_ISSUE_TYPE", "Task"),
    }
