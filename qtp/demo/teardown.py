"""teardown.py - undo a demo run.

Removes the demo's own footprint so the warehouse returns to whatever real state it had
before `qtp demo run`:

  * deletes every ``DEMO-*`` row the seeder created (conforming, defective, quarantine, and
    the demo's pipeline_audit row) - reuses the seeder's reset so the two never drift;
  * optionally stops the Control Hub jobs the live overlay may have started (--stop-jobs).

It only ever touches data it created (the ``DEMO-*`` namespace) - real pipeline output is
left untouched. The WireMock request journal is left for inspection unless --clear-journal.
"""

import psycopg2

from ..common.env import pg_conn_params
from ..common.util import get_logger
from .seed import _reset

log = get_logger()


def _count_demo(cur) -> dict:
    counts = {}
    for table in ("conforming_parts", "defective_parts", "quarantine_log"):
        col = "quarantine_id" if table == "quarantine_log" else "part_id"
        cur.execute(f"SELECT count(*) FROM quality.{table} WHERE {col} LIKE 'DEMO-%'")
        counts[table] = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM quality.pipeline_audit WHERE operator = 'qtp-demo'")
    counts["pipeline_audit"] = cur.fetchone()[0]
    return counts


def run(stop_jobs: bool = False, clear_journal: bool = False) -> int:
    """Remove DEMO-* rows; optionally stop jobs and clear the WireMock journal."""
    conn = psycopg2.connect(**pg_conn_params())
    try:
        with conn, conn.cursor() as cur:
            before = _count_demo(cur)
            _reset(cur)
        removed = sum(before.values())
        log.info("Removed %d DEMO-* row(s): %s", removed,
                 ", ".join(f"{k}={v}" for k, v in before.items()))
    finally:
        conn.close()

    if clear_journal:
        from .evidence import reset_journal

        reset_journal()
        log.info("Cleared WireMock request journal.")

    if stop_jobs:
        try:
            from ..jobs.job_manager import stop

            stop()
        except Exception as exc:  # noqa: BLE001 - stopping jobs needs Control Hub; never fatal
            log.warning("Could not stop jobs (%s).", exc)

    print("  Teardown complete - DEMO-* data removed; real warehouse data untouched.")
    return 0


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="teardown",
                                     description="Remove the demo's DEMO-* footprint.")
    parser.add_argument("--stop-jobs", action="store_true",
                        help="also stop the Control Hub jobs started by the live overlay")
    parser.add_argument("--clear-journal", action="store_true",
                        help="also clear the WireMock request journal")
    args = parser.parse_args(argv)
    try:
        return run(stop_jobs=args.stop_jobs, clear_journal=args.clear_journal)
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
