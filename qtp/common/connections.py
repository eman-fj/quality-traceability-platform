"""Control Hub named Connections (Master Programme Doc §9.2).

"All connections must be defined as named connections in Control Hub: no hardcoded
credentials in pipeline configs. Connection credentials are stored as Control Hub runtime
parameters and injected at job execution time."

Today every pipeline stage sets its DB/API endpoint via a plain pipeline *parameter*
(``${PG_JDBC_URL}`` etc.) - that satisfies "no literal value in the stage config", but the
parameter's own default value is still a plain string baked into the published pipeline
(and into ``qtp pipelines export``'s committed JSON). A Control Hub **Connection** is a
separate, centrally-managed, access-controlled object: a stage references it by ID
(``stage.connection = <id>``) instead of carrying connection details itself at all.

This module creates the connections QTP's stages support (verified against the installed
SDK + this engine's actual connection-type schemas - see PROJECT_REPORT.md) and resolves
them by name, mirroring ``qtp/common/fragments.py``'s pattern. ``apply_connection`` is a
best-effort helper: if the named connection exists it's used; if not, the caller's existing
MANUAL parameter-based wiring is left untouched - so pipelines upgrade to named connections
automatically once they exist, without a flag day / without breaking anything if one is
ever renamed or removed.
"""

from .env import mailhog_smtp, minio_settings, mysql_conn_params, pg_conn_params
from .util import get_logger

log = get_logger()

PREFIX = "manufacturing__"

# name -> (connection_type, in-network jdbc/broker string, env-var source)
_POSTGRES_NAME = PREFIX + "postgres__dev"
_MYSQL_NAME = PREFIX + "mysql__dev"
_MINIO_NAME = PREFIX + "minio__dev"
_KAFKA_NAME = PREFIX + "kafka__dev"


def _find(sch, name: str):
    try:
        return sch.connections.get(name=name)
    except Exception:  # noqa: BLE001 - not found
        return None


def resolve_connection(sch, name: str):
    """The connection object for `name`, or None if it doesn't exist yet."""
    return _find(sch, name)


def apply_connection(stage, sch, name: str, fallback_note: str = "") -> bool:
    """Point `stage` at the named Connection if it exists; else leave MANUAL wiring alone.

    Default ON. A stage bound to a named Connection validated cleanly once the engine had
    settled after a restart - an earlier JDBC_INIT_50/56 failure turned out to be a transient
    post-restart network state, NOT a flaw in the connections (they use the same host/creds as
    the manual params). Set QTP_USE_NAMED_CONNECTIONS=0 as an escape hatch to force the manual
    ${PARAM} wiring (e.g. immediately after an engine restart, before connection pools settle).

    If the named connection doesn't exist (fresh workspace), this falls back to the manual
    wiring the caller already set - so nothing breaks before `qtp connections create` is run.

    Returns True if the connection was applied (caller can skip setting manual params).
    """
    import os

    if os.getenv("QTP_USE_NAMED_CONNECTIONS", "1").strip().lower() not in ("1", "true", "yes"):
        return False  # explicit escape hatch: keep the manual ${PARAM} wiring
    conn = _find(sch, name)
    if conn is None:
        log.info("Named connection '%s' not found; using MANUAL parameters%s.",
                 name, f" ({fallback_note})" if fallback_note else "")
        return False
    stage.connection = conn.id
    log.info("Stage wired to named connection '%s' (%s).", name, conn.id)
    return True


# --------------------------------------------------------------------------- creation
def _build_postgres(sch, engine):
    p = pg_conn_params()
    cb = sch.get_connection_builder()
    conn = cb.build(title=_POSTGRES_NAME, connection_type="STREAMSETS_JDBC",
                    authoring_engine=engine, tags=["industry:manufacturing", "env:dev"])
    cfg = conn.connection_definition.configuration
    # In-network hostname (matches every pipeline's existing PG_JDBC_URL default) - p["host"]
    # from pg_conn_params() is the HOST-remapped address (localhost:5434 etc.), not usable
    # from inside the docker network the engine runs in.
    cfg["connectionString"] = f"jdbc:postgresql://postgres:5432/{p['dbname']}"
    cfg["username"] = p["user"]
    cfg["password"] = p["password"]
    cfg["useCredentials"] = True
    return conn


def _build_mysql(sch, engine):
    m = mysql_conn_params()
    cb = sch.get_connection_builder()
    conn = cb.build(title=_MYSQL_NAME, connection_type="STREAMSETS_JDBC",
                    authoring_engine=engine, tags=["industry:manufacturing", "env:dev"])
    cfg = conn.connection_definition.configuration
    cfg["connectionString"] = (
        f"jdbc:mysql://mysql:3306/{m['database']}?useUnicode=true&characterEncoding=UTF-8"
    )
    cfg["username"] = m["user"]
    cfg["password"] = m["password"]
    cfg["useCredentials"] = True
    return conn


def _build_minio(sch, engine):
    s = minio_settings()
    cb = sch.get_connection_builder()
    conn = cb.build(title=_MINIO_NAME, connection_type="STREAMSETS_AWS_S3",
                    authoring_engine=engine, tags=["industry:manufacturing", "env:dev"])
    cfg = conn.connection_definition.configuration
    cfg["awsConfig.credentialMode"] = "WITH_CREDENTIALS"
    cfg["awsConfig.awsAccessKeyId"] = s["access_key"]
    cfg["awsConfig.awsSecretAccessKey"] = s["secret_key"]
    cfg["awsRegionConfig.regionDefinitionType"] = "SPECIFY_NON_REGIONAL_ENDPOINT"
    cfg["awsRegionConfig.nonRegionalEndpoint"] = "https://minio:9000"  # in-network, TLS
    cfg["awsRegionConfig.signingRegion"] = "US_EAST_1"
    return conn


def _build_kafka(sch, engine):
    cb = sch.get_connection_builder()
    conn = cb.build(title=_KAFKA_NAME, connection_type="STREAMSETS_KAFKA",
                    authoring_engine=engine, tags=["industry:manufacturing", "env:dev"])
    cfg = conn.connection_definition.configuration
    cfg["metadataBrokerList"] = "kafka:29092"  # in-network broker
    cfg["securityConfig.securityOption"] = "PLAINTEXT"
    return conn


_BUILDERS = {
    "postgres": (_POSTGRES_NAME, _build_postgres),
    "mysql":    (_MYSQL_NAME, _build_mysql),
    "minio":    (_MINIO_NAME, _build_minio),
    "kafka":    (_KAFKA_NAME, _build_kafka),
}


def create_all(sch, engine, names=None, replace=False) -> list[str]:
    """Create the named connections that don't already exist (idempotent)."""
    created = []
    for key, (name, build) in _BUILDERS.items():
        if names and key not in names:
            continue
        existing = _find(sch, name)
        if existing is not None:
            if not replace:
                log.info("Connection '%s' already exists; skipping (use --replace to recreate).", name)
                continue
            log.info("Replacing existing connection '%s'.", name)
        conn = build(sch, engine)
        sch.add_connection(conn)
        created.append(name)
        log.info("Created connection '%s' (%s).", name, conn.connection_type)
    return created
