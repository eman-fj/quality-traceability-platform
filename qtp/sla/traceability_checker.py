"""traceability_checker.py — ISO 9001 two-hour traceability SLA enforcement.

A defective part must be traceable back to its raw-material supplier and batch
certificate within **2 hours** of defect detection — a legal requirement under EU
product-liability law and an ISO 9001 audit control. This script proves and enforces
that SLA against the data the `manufacturing__defect_enrichment` pipeline produces:

  * ``check --part <id>`` — reconstructs the full traceability chain
    (part → batch → supplier → certificate) for one part, determines whether
    traceability completed inside the 2-hour window, and emits a structured JSON
    report (the artifact attached to a Jira ticket). On breach it fires a CRITICAL
    alert and auto-creates a Jira compliance ticket.

  * ``scan`` — intended to run on a 15-minute schedule; finds every defective part
    whose traceability breached the 2-hour SLA (or is incomplete and past the
    window) and raises a CRITICAL alert + Jira ticket for each (capped to avoid
    flooding), then prints a summary.

Pure Python + SQL against Postgres `quality.defective_parts`; the CRITICAL alert and
Jira ticket go to the WireMock-stubbed external APIs in the docker environment
(endpoints/credentials come from .env — nothing hardcoded).
"""

import json
from datetime import datetime, timezone
from decimal import Decimal

import psycopg2
import psycopg2.extras
import requests

from ..common.env import pg_conn_params, wiremock_base_url
from ..common.util import get_logger

log = get_logger()

SLA_MINUTES = 120  # ISO 9001 / EU product-liability traceability SLA: 2 hours.

# Columns selected for a defective part, plus the computed traceability duration.
_PART_SELECT = """
    SELECT part_id, line_id, inspection_timestamp, material_batch_id, operator_id,
           shift, defect_codes, defect_category, recommended_action,
           supplier_id, supplier_name, material_grade, certificate_url,
           quarantine_id, traceability_completed_at,
           EXTRACT(EPOCH FROM (traceability_completed_at
                   - inspection_timestamp::timestamptz)) / 60.0 AS duration_minutes,
           EXTRACT(EPOCH FROM (now()
                   - inspection_timestamp::timestamptz)) / 60.0 AS age_minutes
    FROM quality.defective_parts
"""


# --------------------------------------------------------------------------- db
def _connect():
    return psycopg2.connect(**pg_conn_params())


def fetch_part(conn, part_id: str) -> dict | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_PART_SELECT + " WHERE part_id = %s", (part_id,))
        return cur.fetchone()


def fetch_breaches(conn, limit: int = 500) -> list[dict]:
    """Defective parts that breached the SLA: traceability took >2h, or is still
    incomplete and the 2-hour window has already passed since detection."""
    where = """
        WHERE (traceability_completed_at IS NOT NULL
               AND traceability_completed_at - inspection_timestamp::timestamptz
                   > make_interval(mins => %(sla)s))
           OR (traceability_completed_at IS NULL
               AND now() - inspection_timestamp::timestamptz
                   > make_interval(mins => %(sla)s))
        ORDER BY duration_minutes DESC NULLS FIRST
        LIMIT %(limit)s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_PART_SELECT + where, {"sla": SLA_MINUTES, "limit": limit})
        return cur.fetchall()


# ----------------------------------------------------------------------- report
def _iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _json_default(o):
    """JSON encoder fallback: psycopg2 returns NUMERIC as Decimal and timestamps as
    datetime — coerce both to JSON-friendly types."""
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return _iso(o)
    return str(o)


def build_report(row: dict) -> dict:
    """Shape a part row into the structured traceability + SLA report."""
    completed = row["traceability_completed_at"]
    duration = row["duration_minutes"]
    if duration is not None:
        duration = float(duration)  # psycopg2 returns NUMERIC as Decimal
    if completed is None:
        status = "BREACHED" if (row["age_minutes"] or 0) > SLA_MINUTES else "INCOMPLETE"
    else:
        status = "MET" if (duration is not None and duration <= SLA_MINUTES) else "BREACHED"

    return {
        "part_id": row["part_id"],
        "checked_at": _iso(datetime.now(timezone.utc)),
        "traceability_chain": {
            "part": {
                "part_id": row["part_id"],
                "line_id": row["line_id"],
                "shift": row["shift"],
                "operator_id": row["operator_id"],
                "inspection_timestamp": _iso(row["inspection_timestamp"]),
                "defect_codes": row["defect_codes"],
                "defect_category": row["defect_category"],
                "recommended_action": row["recommended_action"],
            },
            "material_batch": {
                "material_batch_id": row["material_batch_id"],
                "material_grade": row["material_grade"],
            },
            "supplier": {
                "supplier_id": row["supplier_id"],
                "supplier_name": row["supplier_name"],
            },
            "certificate_url": row["certificate_url"],
        },
        "quarantine_id": row["quarantine_id"],
        "sla": {
            "detected_at": _iso(row["inspection_timestamp"]),
            "traceability_completed_at": _iso(completed),
            "duration_minutes": round(duration, 1) if duration is not None else None,  # float
            "sla_minutes": SLA_MINUTES,
            "status": status,
        },
        "compliant": status == "MET",
    }


# ------------------------------------------------------------------ alert + jira
def fire_alert_and_ticket(report: dict) -> dict:
    """On an SLA breach: send a CRITICAL alert and auto-create a Jira ticket.

    Both go to the WireMock-stubbed APIs. Failures are logged but never crash the
    check — a missing webhook must not stop SLA monitoring.
    """
    base = wiremock_base_url()
    part_id = report["part_id"]
    duration = report["sla"]["duration_minutes"]
    summary = "Traceability SLA breach: {} ({} min > {} min)".format(
        part_id, duration, SLA_MINUTES
    )
    result = {"alert_sent": False, "jira_ticket": None}

    # CRITICAL alert (severity-routed externally; here the stubbed notification API).
    try:
        requests.post(
            base + "/alerts",
            json={
                "severity": "CRITICAL",
                "type": "traceability_sla_breach",
                "part_id": part_id,
                "duration_minutes": duration,
                "message": summary,
            },
            timeout=5,
        )
        result["alert_sent"] = True
    except requests.RequestException as exc:
        log.warning("CRITICAL alert POST failed for %s: %s", part_id, exc)

    # Auto-create the Jira compliance ticket, attaching the JSON report.
    try:
        resp = requests.post(
            base + "/jira/issue",
            json={
                "fields": {
                    "project": {"key": "QUAL"},
                    "issuetype": {"name": "Compliance"},
                    "summary": summary,
                    "description": json.dumps(report, default=_json_default),
                }
            },
            timeout=5,
        )
        if resp.ok:
            result["jira_ticket"] = resp.json().get("key")
    except requests.RequestException as exc:
        log.warning("Jira ticket creation failed for %s: %s", part_id, exc)

    return result


# ---------------------------------------------------------------------- actions
def check_part(part_id: str) -> int:
    """Mode 1: full traceability + SLA report for one part. Prints JSON."""
    conn = _connect()
    try:
        row = fetch_part(conn, part_id)
    finally:
        conn.close()

    if row is None:
        log.error("No defective part found with part_id=%s", part_id)
        return 1

    report = build_report(row)
    if report["sla"]["status"] == "BREACHED":
        report["enforcement"] = fire_alert_and_ticket(report)
        log.warning(
            "SLA BREACH on %s (%.1f min) — alert sent=%s, jira=%s",
            part_id, report["sla"]["duration_minutes"] or -1,
            report["enforcement"]["alert_sent"], report["enforcement"]["jira_ticket"],
        )
    else:
        log.info("SLA %s on %s (%s min).", report["sla"]["status"], part_id,
                 report["sla"]["duration_minutes"])

    print(json.dumps(report, indent=2, default=_json_default))
    return 0


def scan(max_alerts: int = 10) -> int:
    """Mode 2: poll for SLA breaches and raise alerts (capped). Prints a summary."""
    conn = _connect()
    try:
        breaches = fetch_breaches(conn)
    finally:
        conn.close()

    if not breaches:
        log.info("No traceability SLA breaches — all defective parts traced within %d min.",
                 SLA_MINUTES)
        return 0

    log.warning("%d defective part(s) breached the %d-min traceability SLA.",
                len(breaches), SLA_MINUTES)
    fired = 0
    for row in breaches:
        report = build_report(row)
        # An incomplete breach (traceability never completed) has no duration — show the
        # SLA status instead of trying to format None as a number.
        duration = report["sla"]["duration_minutes"]
        dur = f"{duration:>8}" if duration is not None else f"{report['sla']['status']:>8}"
        if fired < max_alerts:
            enforcement = fire_alert_and_ticket(report)
            fired += 1
            print("BREACH  {:30s} {} min  alert={} jira={}".format(
                report["part_id"], dur,
                enforcement["alert_sent"], enforcement["jira_ticket"]))
        else:
            print("BREACH  {:30s} {} min  (alert suppressed - cap reached)".format(
                report["part_id"], dur))

    log.info("Scan complete: %d breach(es), %d alert(s)/ticket(s) raised (cap %d).",
             len(breaches), fired, max_alerts)
    return 0


# ------------------------------------------------------------------------- main
def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="traceability_checker",
        description="ISO 9001 two-hour traceability SLA checker for defective parts.",
    )
    sub = parser.add_subparsers(dest="mode", required=True, metavar="<mode>")

    p_check = sub.add_parser("check", help="report + enforce SLA for one part")
    p_check.add_argument("--part", required=True, help="part_id to check")

    p_scan = sub.add_parser("scan", help="poll for SLA breaches and raise alerts")
    p_scan.add_argument("--max-alerts", type=int, default=10,
                        help="cap alerts/tickets raised per scan (default 10)")

    args = parser.parse_args(argv)
    try:
        if args.mode == "check":
            return check_part(args.part)
        return scan(args.max_alerts)
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
