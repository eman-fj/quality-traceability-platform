"""preflight.py — pre-demo service health board.

Probes every service the demonstration touches and prints a one-line-per-service status
board (GREEN / RED), so a presenter can confirm the stack is up *before* running
`qtp demo run`. Each probe is host-side (the same remapped ports the SDK scripts use) and
fully isolated — one service being down never masks the status of the others.

Required services (a RED here means the demo cannot fully run): Postgres, WireMock.
Everything else is reported but treated as optional/degradable, matching how the demo and
the SDK scripts degrade gracefully when a sink is missing.
"""

import os
import socket
import time
from urllib.parse import urlparse

from ..common.env import (
    mailhog_smtp,
    minio_settings,
    mysql_conn_params,
    pg_conn_params,
    wiremock_base_url,
)
from ..common.util import get_logger

log = get_logger()

# Services that must be up for the demo's compliance suite to run end-to-end. The rest are
# reported but the demo degrades gracefully without them (matching the SDK scripts).
REQUIRED = {"postgres", "wiremock"}


def _tcp_ok(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _http_ok(url: str, timeout: float = 3.0, insecure: bool = False) -> bool:
    """True if an HTTP GET returns any response (even 4xx — the port is alive)."""
    import requests

    if insecure:
        # MinIO serves a self-signed cert in this stack; the "unverified HTTPS" warning is
        # expected and noisy on a health probe — silence it.
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        resp = requests.get(url, timeout=timeout, verify=not insecure)
        return resp.status_code < 500
    except requests.RequestException:
        return False


# --------------------------------------------------------------------------- probes
def _probe_postgres() -> tuple[bool, str]:
    import psycopg2

    p = pg_conn_params()
    try:
        conn = psycopg2.connect(connect_timeout=3, **p)
        conn.close()
        return True, f"{p['host']}:{p['port']}/{p['dbname']}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{p['host']}:{p['port']} ({exc})"


def _probe_mysql() -> tuple[bool, str]:
    p = mysql_conn_params()
    try:
        import pymysql

        conn = pymysql.connect(connect_timeout=3, **p)
        conn.close()
        return True, f"{p['host']}:{p['port']}/{p['database']}"
    except Exception as exc:  # noqa: BLE001
        # Fall back to a bare TCP check so a missing pymysql doesn't read as "down".
        if _tcp_ok(p["host"], p["port"]):
            return True, f"{p['host']}:{p['port']} (port open; {type(exc).__name__})"
        return False, f"{p['host']}:{p['port']} ({exc})"


def _probe_wiremock() -> tuple[bool, str]:
    base = wiremock_base_url()
    ok = _http_ok(base + "/__admin/mappings")
    return ok, base


def _probe_kafka() -> tuple[bool, str]:
    host = os.getenv("KAFKA_HOST", "localhost")
    port = int(os.getenv("KAFKA_PORT", "9092"))
    return _tcp_ok(host, port), f"{host}:{port}"


def _probe_minio() -> tuple[bool, str]:
    endpoint = minio_settings()["endpoint_url"]
    # MinIO is served over TLS with a self-signed cert in this stack — skip verification.
    ok = _http_ok(endpoint + "/minio/health/live", insecure=True)
    if not ok:  # fall back to TCP so a cert quirk doesn't read as "down"
        parsed = urlparse(endpoint)
        ok = _tcp_ok(parsed.hostname or "localhost", parsed.port or 9000)
    return ok, endpoint


def _probe_mailhog() -> tuple[bool, str]:
    host, smtp_port = mailhog_smtp()
    ui_port = int(os.getenv("MAILHOG_UI_PORT", "8025"))
    ok = _http_ok(f"http://{host}:{ui_port}/api/v2/messages") or _tcp_ok(host, smtp_port)
    return ok, f"{host} (smtp {smtp_port} / ui {ui_port})"


def _probe_grafana() -> tuple[bool, str]:
    host = os.getenv("GRAFANA_HOST", "localhost")
    port = int(os.getenv("GRAFANA_PORT", "3000"))
    return _http_ok(f"http://{host}:{port}/api/health"), f"{host}:{port}"


def _probe_prometheus() -> tuple[bool, str]:
    host = os.getenv("PROMETHEUS_HOST", "localhost")
    port = int(os.getenv("PROMETHEUS_PORT", "9090"))
    return _http_ok(f"http://{host}:{port}/-/healthy"), f"{host}:{port}"


def _probe_controlhub() -> tuple[bool, str]:
    """Optional: only meaningful if Control Hub creds are configured (for live overlays)."""
    if not os.getenv("SCH_URL"):
        return True, "not configured (offline demo — fine)"
    parsed = urlparse(os.getenv("SCH_URL"))
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return _tcp_ok(host, port, timeout=4.0), os.getenv("SCH_URL")


_PROBES = [
    ("postgres", "quality warehouse", _probe_postgres),
    ("mysql", "ERP (suppliers/batches)", _probe_mysql),
    ("wiremock", "classify/quarantine/alerts/jira", _probe_wiremock),
    ("kafka", "inspection event bus", _probe_kafka),
    ("minio", "object storage (reports/DLQ)", _probe_minio),
    ("mailhog", "email sink", _probe_mailhog),
    ("grafana", "dashboards", _probe_grafana),
    ("prometheus", "metrics", _probe_prometheus),
    ("controlhub", "StreamSets SDK target", _probe_controlhub),
]


def gather() -> list[dict]:
    """Run every probe and return a list of result dicts (used by the demo + evidence)."""
    results = []
    for name, desc, probe in _PROBES:
        started = time.monotonic()
        try:
            ok, detail = probe()
        except Exception as exc:  # noqa: BLE001 - a probe must never crash preflight
            ok, detail = False, str(exc)
        results.append({
            "service": name,
            "description": desc,
            "ok": ok,
            "required": name in REQUIRED,
            "detail": detail,
            "ms": int((time.monotonic() - started) * 1000),
        })
    return results


def print_board(results: list[dict]) -> None:
    print("\n  SERVICE      STATUS   DETAIL")
    print("  " + "-" * 64)
    for r in results:
        mark = "GREEN" if r["ok"] else ("RED  " if r["required"] else "warn ")
        flag = "*" if r["required"] else " "
        print(f"  {r['service']:<12}{flag}{mark:<6} {r['detail']}")
    print("  " + "-" * 64)
    print("  (* = required for the demo to run end-to-end)\n")


def run() -> int:
    """`qtp preflight run` — print the health board; non-zero exit if a REQUIRED service is down."""
    results = gather()
    print_board(results)
    down_required = [r["service"] for r in results if r["required"] and not r["ok"]]
    if down_required:
        log.error("Preflight: required service(s) down: %s. Start the stack with "
                  "`docker compose up -d` and re-run.", ", ".join(down_required))
        return 1
    degraded = [r["service"] for r in results if not r["ok"]]
    if degraded:
        log.warning("Preflight: %d optional service(s) degraded: %s (demo will still run).",
                    len(degraded), ", ".join(degraded))
    else:
        log.info("Preflight: all services GREEN.")
    return 0


def main(argv=None) -> int:
    import argparse

    argparse.ArgumentParser(prog="preflight",
                            description="QTP service health board.").parse_args(argv)
    return run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
