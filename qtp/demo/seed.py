"""seed.py - deterministic "golden thread" demo data.

Seeds the quality warehouse with a small, fixed scenario engineered so every compliance
control has something concrete to demonstrate, the same way every run:

  * **DEMO-CRIT-0001** - a CRITICAL surface-crack part on line_1, traced to its supplier
    in 5 minutes (SLA *MET*). The hero of `qtp sla check` - a complete chain ending at a
    **SUSPENDED** supplier (SUP-05), which is exactly the kind of finding the platform exists
    to surface.
  * **DEMO-SLA-0002** - a CRITICAL part on line_2 detected >2 hours ago whose traceability
    was never completed: a live **SLA breach** for `qtp sla scan` to catch and escalate.
  * **line_3** seeded hot (>2% defect rate) so `qtp monitor defect-rate` fires a CRITICAL
    alert + Jira ticket. Most of line_3's defects trace back to the same SUSPENDED supplier,
    so the shift report's supplier-correlation also points at SUP-05 - one coherent story.
  * a full shift-A dataset (conforming + defective) so `qtp report shift` has real numbers.

Everything is namespaced ``DEMO-*`` and the seed is idempotent (upsert; ``--reset`` wipes
prior demo rows first), so it can be run repeatedly and never collides with real pipeline
output. Writes only to Postgres; the supplier/batch facts mirror the MySQL ERP seed so no
live ERP join is needed to stage the scenario.
"""

from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

from ..common.env import pg_conn_params
from ..common.util import get_logger

log = get_logger()

# Mirror of the MySQL ERP seed (docker/mysql/init.sql) - the traceability facts the live
# defect-enrichment pipeline would join in. Keeps the demo seed self-contained.
ERP = {
    "BATCH-1001": ("SUP-01", "NordStahl Components GmbH", "S355J2", "ACCREDITED"),
    "BATCH-1003": ("SUP-02", "Lyon Précision Métaux", "42CrMo4", "ACCREDITED"),
    "BATCH-1005": ("SUP-03", "Iberia Forge S.A.", "C45E", "PROVISIONAL"),
    "BATCH-1007": ("SUP-04", "Baltic Alloy OÜ", "AlSi10Mg", "ACCREDITED"),
    "BATCH-1009": ("SUP-05", "Adriatic Metalworks d.o.o", "EN-GJL250", "SUSPENDED"),
}
CERT = "https://certs.example/erp/{}.pdf"

# Defect-code -> (category, recommended_action), per the brief's classify contract.
CLASSIFY = {
    "SUR-001": ("CRITICAL", "SCRAP"), "MAT-001": ("CRITICAL", "SCRAP"),
    "DIM-001": ("MAJOR", "REWORK"), "DIM-002": ("MAJOR", "REWORK"), "MAT-002": ("MAJOR", "REWORK"),
    "SUR-002": ("MINOR", "HOLD"), "ASS-001": ("MINOR", "HOLD"),
}

HERO_PART = "DEMO-CRIT-0001"
BREACH_PART = "DEMO-SLA-0002"
HOT_LINE = "line_3"
SHIFT = "A"
SCHEMA_VERSION = 1


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _measurements(defective: bool) -> dict:
    return {
        "length_mm": 120.0, "diameter_mm": 25.0,
        "surface_score": 0.92 if defective else 0.10,
        "hardness_hrc": 39.0 if defective else 52.0,
    }


def _conforming_row(part_id, line, batch, op, when) -> tuple:
    return (part_id, line, _iso(when), batch, op, SHIFT,
            psycopg2.extras.Json(_measurements(False)), SCHEMA_VERSION)


def _defective_row(part_id, line, batch, op, detected, code, *,
                   completed_after_min, quarantine_id) -> tuple:
    supplier_id, supplier_name, grade, _status = ERP[batch]
    category, action = CLASSIFY[code]
    completed = None if completed_after_min is None else detected + timedelta(minutes=completed_after_min)
    return (part_id, line, _iso(detected), batch, op, SHIFT,
            psycopg2.extras.Json([code]), category, action,
            supplier_id, supplier_name, grade, CERT.format(batch),
            quarantine_id, completed, SCHEMA_VERSION)


_CONFORMING_SQL = """
    INSERT INTO quality.conforming_parts
        (part_id, line_id, inspection_timestamp, material_batch_id, operator_id, shift,
         measurements, schema_version)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (part_id) DO UPDATE SET
        line_id=EXCLUDED.line_id, inspection_timestamp=EXCLUDED.inspection_timestamp,
        material_batch_id=EXCLUDED.material_batch_id, operator_id=EXCLUDED.operator_id,
        shift=EXCLUDED.shift, measurements=EXCLUDED.measurements,
        schema_version=EXCLUDED.schema_version
"""

_DEFECTIVE_SQL = """
    INSERT INTO quality.defective_parts
        (part_id, line_id, inspection_timestamp, material_batch_id, operator_id, shift,
         defect_codes, defect_category, recommended_action,
         supplier_id, supplier_name, material_grade, certificate_url,
         quarantine_id, traceability_completed_at, schema_version)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (part_id) DO UPDATE SET
        line_id=EXCLUDED.line_id, inspection_timestamp=EXCLUDED.inspection_timestamp,
        material_batch_id=EXCLUDED.material_batch_id, operator_id=EXCLUDED.operator_id,
        shift=EXCLUDED.shift, defect_codes=EXCLUDED.defect_codes,
        defect_category=EXCLUDED.defect_category, recommended_action=EXCLUDED.recommended_action,
        supplier_id=EXCLUDED.supplier_id, supplier_name=EXCLUDED.supplier_name,
        material_grade=EXCLUDED.material_grade, certificate_url=EXCLUDED.certificate_url,
        quarantine_id=EXCLUDED.quarantine_id,
        traceability_completed_at=EXCLUDED.traceability_completed_at,
        schema_version=EXCLUDED.schema_version
"""

_QUARANTINE_SQL = """
    INSERT INTO quality.quarantine_log (quarantine_id, part_id, status, retry_count)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (quarantine_id) DO UPDATE SET
        part_id=EXCLUDED.part_id, status=EXCLUDED.status, retry_count=EXCLUDED.retry_count
"""

_AUDIT_SQL = """
    INSERT INTO quality.pipeline_audit (pipeline_name, run_ts, operator, environment, audit_metrics)
    VALUES (%s, %s, %s, %s, %s)
"""


def _build_dataset(now: datetime):
    """Return (conforming_rows, defective_rows, quarantine_rows) for the scenario."""
    conforming, defective, quarantine = [], [], []

    # 1) Hero CRITICAL on line_1 - traced in 5 min (SLA MET), to the SUSPENDED supplier.
    defective.append(_defective_row(
        HERO_PART, "line_1", "BATCH-1009", "OP-101", now - timedelta(minutes=12), "SUR-001",
        completed_after_min=5, quarantine_id="DEMO-Q-0001"))
    quarantine.append(("DEMO-Q-0001", HERO_PART, "QUARANTINED", 0))

    # 2) SLA breach on line_2 - detected 190 min ago, traceability never completed.
    defective.append(_defective_row(
        BREACH_PART, "line_2", "BATCH-1005", "OP-102", now - timedelta(minutes=190), "MAT-001",
        completed_after_min=None, quarantine_id="DEMO-Q-0002"))
    quarantine.append(("DEMO-Q-0002", BREACH_PART, "QUARANTINED", 1))

    # 3) Hot line_3 - 8 defects (5 MAJOR quarantined, 3 MINOR), most from SUP-05, all traced
    #    inside the SLA so they raise a defect-rate breach without polluting the SLA scan.
    hot_defects = [
        ("DEMO-L3-MAJ-001", "BATCH-1009", "DIM-001"),
        ("DEMO-L3-MAJ-002", "BATCH-1009", "DIM-002"),
        ("DEMO-L3-MAJ-003", "BATCH-1009", "MAT-002"),
        ("DEMO-L3-MAJ-004", "BATCH-1005", "DIM-001"),
        ("DEMO-L3-MAJ-005", "BATCH-1001", "MAT-002"),
        ("DEMO-L3-MIN-001", "BATCH-1009", "SUR-002"),
        ("DEMO-L3-MIN-002", "BATCH-1005", "ASS-001"),
        ("DEMO-L3-MIN-003", "BATCH-1001", "SUR-002"),
    ]
    for i, (pid, batch, code) in enumerate(hot_defects):
        category = CLASSIFY[code][0]
        qid = f"DEMO-Q-L3-{i:03d}" if category == "MAJOR" else None
        defective.append(_defective_row(
            pid, HOT_LINE, batch, "OP-103", now - timedelta(minutes=20 + i * 3), code,
            completed_after_min=8, quarantine_id=qid))
        if qid:
            quarantine.append((qid, pid, "QUARANTINED", 0))

    # 4) Conforming volume so each line's window stays realistic and the shift report has a
    #    high conforming rate. line_3 stays hot (8 / ~68 ≈ 12%); line_1/line_2 well under 2%.
    volume = {"line_1": 80, "line_2": 80, "line_3": 60}
    batches = list(ERP)
    for line, count in volume.items():
        for n in range(count):
            pid = f"DEMO-{line.upper()}-CONF-{n:04d}"
            batch = batches[n % len(batches)]
            when = now - timedelta(minutes=(n % 90) + 1)
            conforming.append(_conforming_row(pid, line, batch, "OP-10%d" % (n % 5 + 1), when))

    return conforming, defective, quarantine


def _reset(cur) -> None:
    cur.execute("DELETE FROM quality.quarantine_log WHERE quarantine_id LIKE 'DEMO-%'")
    cur.execute("DELETE FROM quality.defective_parts  WHERE part_id LIKE 'DEMO-%'")
    cur.execute("DELETE FROM quality.conforming_parts WHERE part_id LIKE 'DEMO-%'")
    cur.execute("DELETE FROM quality.pipeline_audit   WHERE operator = 'qtp-demo'")


def seed(reset: bool = True) -> dict:
    """Seed the golden-thread scenario. Returns a summary dict for the runner/evidence."""
    import json

    now = datetime.now(timezone.utc)
    conforming, defective, quarantine = _build_dataset(now)

    conn = psycopg2.connect(**pg_conn_params())
    try:
        with conn, conn.cursor() as cur:
            if reset:
                _reset(cur)
            psycopg2.extras.execute_batch(cur, _CONFORMING_SQL, conforming)
            psycopg2.extras.execute_batch(cur, _DEFECTIVE_SQL, defective)
            psycopg2.extras.execute_batch(cur, _QUARANTINE_SQL, quarantine)
            # One audit row so the "audit log on every run" control has evidence.
            cur.execute(_AUDIT_SQL, (
                "manufacturing__defect_enrichment", now, "qtp-demo", "dev",
                json.dumps({"defect_count": len(defective), "critical_count": 2,
                            "quarantine_registrations": len(quarantine),
                            "traceability_lookups": len(defective), "source": "qtp demo seed"}),
            ))
    finally:
        conn.close()

    summary = {
        "seeded_at": now.isoformat(),
        "conforming": len(conforming),
        "defective": len(defective),
        "quarantined": len(quarantine),
        "hero_part": HERO_PART,
        "breach_part": BREACH_PART,
        "hot_line": HOT_LINE,
        "shift": SHIFT,
        "date": now.date().isoformat(),
    }
    log.info("Seeded golden thread: %d conforming, %d defective, %d quarantined "
             "(hero=%s, breach=%s, hot=%s).",
             summary["conforming"], summary["defective"], summary["quarantined"],
             HERO_PART, BREACH_PART, HOT_LINE)
    return summary


def run(reset: bool = True) -> int:
    """`qtp demo seed` - seed the scenario and print a short summary."""
    s = seed(reset=reset)
    print(f"\n  Golden thread seeded for shift {s['shift']} on {s['date']}:")
    print(f"    hero CRITICAL part (SLA met) : {s['hero_part']}  -> sla check --part {s['hero_part']}")
    print(f"    SLA-breach part (>2h, untraced): {s['breach_part']}")
    print(f"    hot line (defect rate >2%)   : {s['hot_line']}")
    print(f"    conforming={s['conforming']}  defective={s['defective']}  quarantined={s['quarantined']}\n")
    return 0


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="demo-seed",
                                     description="Seed the QTP golden-thread demo scenario.")
    parser.add_argument("--no-reset", action="store_true",
                        help="upsert without wiping prior DEMO-* rows first")
    args = parser.parse_args(argv)
    try:
        return run(reset=not args.no_reset)
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
