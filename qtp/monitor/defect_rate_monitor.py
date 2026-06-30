"""defect_rate_monitor.py — per-line defect-rate compliance check.

ISO 9001 compliance control: if a production line's defect rate exceeds 2% it must raise a
CRITICAL alert (the brief's "CRITICAL: defect rate above 2%"). This check computes each
line's defect rate over a rolling window of its most recent parts and, for any line above
the threshold, fires a CRITICAL alert and auto-creates a Jira quality-investigation ticket.

Intended to run on a short schedule (e.g. every few minutes). Pure Python + SQL over the
data the StreamSets pipelines produce; alert + Jira go to the WireMock-stubbed APIs in the
docker environment. Nothing hardcoded — endpoints come from .env.
"""

import json

import psycopg2
import psycopg2.extras
import requests

from ..common.env import pg_conn_params, wiremock_base_url
from ..common.util import get_logger

log = get_logger()

THRESHOLD_PCT = 2.0     # ISO 9001 line defect-rate ceiling.
DEFAULT_WINDOW = 100    # rolling window: most recent N parts per line.

# Rolling per-line defect rate over the most recent <window> parts of each line.
_RATE_QUERY = """
    WITH recent AS (
        SELECT line_id, inspection_timestamp::timestamptz AS ts, 0 AS is_defect
            FROM quality.conforming_parts
        UNION ALL
        SELECT line_id, inspection_timestamp::timestamptz, 1
            FROM quality.defective_parts
    ),
    ranked AS (
        SELECT line_id, is_defect,
               row_number() OVER (PARTITION BY line_id ORDER BY ts DESC) AS rn
        FROM recent
    )
    SELECT line_id,
           count(*)                                          AS window_size,
           sum(is_defect)                                    AS defects,
           round(100.0 * sum(is_defect) / count(*), 2)       AS defect_rate
    FROM ranked
    WHERE rn <= %(window)s
    GROUP BY line_id
    ORDER BY defect_rate DESC
"""


def line_defect_rates(conn, window: int) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_RATE_QUERY, {"window": window})
        return [dict(r) for r in cur.fetchall()]


def fire_alert_and_ticket(line_id: str, defect_rate: float, window_size: int) -> dict:
    """CRITICAL alert + Jira ticket for a line over the defect-rate threshold."""
    base = wiremock_base_url()
    summary = (f"Defect rate breach: line {line_id} at {defect_rate}% "
               f"(> {THRESHOLD_PCT}% over last {window_size} parts)")
    result = {"alert_sent": False, "jira_ticket": None}
    try:
        requests.post(base + "/alerts", json={
            "severity": "CRITICAL", "type": "defect_rate_breach",
            "line_id": line_id, "defect_rate": defect_rate, "message": summary}, timeout=5)
        result["alert_sent"] = True
    except requests.RequestException as exc:
        log.warning("Alert POST failed for %s: %s", line_id, exc)
    try:
        resp = requests.post(base + "/jira/issue", json={"fields": {
            "project": {"key": "QUAL"}, "issuetype": {"name": "Compliance"},
            "summary": summary,
            "description": json.dumps({"line_id": line_id, "defect_rate": defect_rate,
                                       "threshold": THRESHOLD_PCT, "window": window_size})}},
            timeout=5)
        if resp.ok:
            result["jira_ticket"] = resp.json().get("key")
    except requests.RequestException as exc:
        log.warning("Jira ticket creation failed for %s: %s", line_id, exc)
    return result


def check(window: int = DEFAULT_WINDOW, threshold: float = THRESHOLD_PCT) -> int:
    conn = psycopg2.connect(**pg_conn_params())
    try:
        rates = line_defect_rates(conn, window)
    finally:
        conn.close()

    if not rates:
        log.info("No parts found — nothing to evaluate.")
        return 0

    breaches = 0
    print(f"Per-line defect rate (last {window} parts; threshold {threshold}%):")
    for r in rates:
        rate = float(r["defect_rate"])
        over = rate > threshold
        status = "BREACH" if over else "ok"
        line = f"  {r['line_id']:10s} {rate:6.2f}%  ({r['defects']}/{r['window_size']})  {status}"
        if over:
            enf = fire_alert_and_ticket(r["line_id"], rate, int(r["window_size"]))
            line += f"  alert={enf['alert_sent']} jira={enf['jira_ticket']}"
            breaches += 1
        print(line)

    log.info("Defect-rate check: %d line(s) over %.1f%% threshold.", breaches, threshold)
    return 0


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="defect_rate_monitor",
        description="Per-line defect-rate compliance check (CRITICAL alert if > threshold).")
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW,
                        help=f"rolling window of recent parts per line (default {DEFAULT_WINDOW})")
    parser.add_argument("--threshold", type=float, default=THRESHOLD_PCT,
                        help=f"defect-rate %% ceiling (default {THRESHOLD_PCT})")
    args = parser.parse_args(argv)
    try:
        return check(args.window, args.threshold)
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
