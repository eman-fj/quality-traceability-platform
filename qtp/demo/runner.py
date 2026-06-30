"""runner.py — `qtp demo run`, the single-command end-to-end demonstration.

Runs the whole ISO 9001 story as a sequence of narrated stages and finishes with a
compliance scorecard mapped to the brief's controls and the scoring rubric:

    preflight -> seed golden thread -> defect-rate monitor -> SLA scan ->
    SLA chain (hero part) -> shift report -> evidence pack -> scorecard

The default run is fully offline (no Control Hub / no live engine required): it seeds a
deterministic scenario and exercises the real compliance scripts against it, so the demo
is identical every time. Two opt-in live overlays sit on top:

    --inject-critical   publish a real CRITICAL inspection event to Kafka (proves ingest)
    --start-jobs        start the Control Hub jobs so the live pipeline processes it

Every stage is isolated: a failure is recorded and the run continues, so the scorecard
always prints and a single dead sink never aborts the presentation.
"""

import os
import sys
import time
from datetime import date as date_cls

from ..common.util import get_logger
from . import evidence as evidence_mod
from . import preflight as preflight_mod
from . import seed as seed_mod

log = get_logger()

_USE_COLOR = sys.stdout.isatty() and not os.getenv("NO_COLOR")


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def _banner(step: int, total: int, title: str, control: str) -> None:
    bar = "=" * 70
    print("\n" + _c("36", bar))
    print(_c("1;36", f"  STAGE {step}/{total} | {title}"))
    print(_c("36", f"  demonstrates: {control}"))
    print(_c("36", bar))


class _Stages:
    """Tracks per-stage outcomes to compute the exit code. Optional stages (the live
    overlays) are recorded but don't fail the run if they can't complete."""

    def __init__(self):
        self.results: list[dict] = []

    def record(self, name: str, ok: bool, detail: str = "", optional: bool = False) -> None:
        self.results.append({"stage": name, "ok": ok, "detail": detail, "optional": optional})

    @property
    def critical_failures(self) -> list[dict]:
        return [r for r in self.results if not r["ok"] and not r["optional"]]


# ------------------------------------------------------------------ live overlays
def _critical_event() -> dict:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    return {
        "part_id": f"LIVE-CRIT-{now.strftime('%H%M%S')}",
        "line_id": "line_1", "inspection_timestamp": now.isoformat(),
        "measurements": {"surface_score": 0.97, "hardness_hrc": 37.0},
        "defect_codes": ["SUR-001"], "material_batch_id": "BATCH-1009",
        "operator_id": "OP-101", "shift": "A", "schema_version": 1,
    }


def _inject_via_docker(event: dict, topic: str) -> tuple[bool, str]:
    """Fallback path: publish through the Kafka container's console producer (no host
    kafka client needed — only the Docker CLI, which is already present since the stack runs)."""
    import json
    import subprocess

    container = os.getenv("KAFKA_CONTAINER", "qtp-kafka")
    cmd = ["docker", "exec", "-i", container, "kafka-console-producer",
           "--bootstrap-server", "localhost:29092", "--topic", topic]
    try:
        proc = subprocess.run(cmd, input=(json.dumps(event) + "\n").encode("utf-8"),
                              capture_output=True, timeout=25)
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return False, f"docker inject failed ({exc})"
    if proc.returncode == 0:
        return True, f"published {event['part_id']} (SUR-001) to {topic} via {container}"
    return False, f"docker inject failed: {proc.stderr.decode(errors='replace').strip()[:200]}"


def inject_critical() -> tuple[bool, str]:
    """Publish one real CRITICAL inspection event to Kafka. Uses the host kafka client if
    present, else falls back to the Kafka container's console producer via Docker."""
    import json

    topic = os.getenv("INSPECTIONS_TOPIC", "quality.inspections")
    event = _critical_event()
    try:
        from kafka import KafkaProducer
    except Exception:  # noqa: BLE001 - no host kafka client; use the Docker fallback
        return _inject_via_docker(event, topic)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", f"localhost:{os.getenv('KAFKA_PORT', '9092')}")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all", request_timeout_ms=8000, api_version_auto_timeout_ms=8000)
        producer.send(topic, key="line_1", value=event)
        producer.flush(timeout=8)
        producer.close(timeout=4)
        return True, f"published {event['part_id']} (SUR-001) to {topic} on {bootstrap}"
    except Exception as exc:  # noqa: BLE001 - host publish failed; try the Docker fallback
        ok, detail = _inject_via_docker(event, topic)
        return (ok, detail) if ok else (False, f"Kafka publish failed: {exc}")


def start_jobs() -> tuple[bool, str]:
    """Start the Control Hub jobs for the core flows (creating them if missing). Live overlay.

    Delegates to ``qtp.jobs.job_manager`` (verified against the installed SDK 7.x job API:
    ``sch.get_job_builder().build`` + ``sch.add_job`` + ``sch.start_job``). Opt-in and never
    required for the deterministic demo to pass — needs a reachable engine.
    """
    try:
        from ..jobs.job_manager import DEFAULT_FLOWS, start

        rc = start(DEFAULT_FLOWS, wait=False, create_missing=True)
        if rc == 0:
            return True, "started core jobs: " + ", ".join(DEFAULT_FLOWS)
        return False, "job start returned non-zero (see log above)"
    except Exception as exc:  # noqa: BLE001
        return False, f"Control Hub job start failed ({exc}); deterministic demo unaffected"


# ----------------------------------------------------------------------- summary
def _print_summary(bundle: dict, paths: dict) -> None:
    """Print the compliance result for the run: the ISO 9001 controls and where the
    evidence lives. (No stage checklist or rubric — the detail is in the evidence pack.)"""
    controls = bundle.get("compliance_controls", [])
    if not controls:
        return
    print("\n" + _c("36", "=" * 70))
    print(_c("1;36", "  Compliance result (ISO 9001 controls)"))
    print(_c("36", "=" * 70) + "\n")
    for c in controls:
        mark = _c("32", "PASS") if c["status"] == "PASS" else _c("31", "FAIL")
        print(f"  {mark}  {c['control']}")
        print(f"        {c['evidence']}")
    passed = sum(1 for c in controls if c["status"] == "PASS")
    line = f"{passed}/{len(controls)} controls passed"
    if paths.get("html"):
        line += f"  |  evidence: {paths['html']}"
    print("\n  " + _c("1;36", line) + "\n")


# ----------------------------------------------------------------------------- run
def run(*, seed_data=True, reset=True, shift="A", date=None, email=None,
        inject=False, jobs=False, keep_journal=False, evidence_dir=None,
        skip=None, settle=2.0) -> int:
    date = date or date_cls.today().isoformat()
    skip = set(skip or [])
    stages = _Stages()

    # Stage plan (kept in order; live overlays are inserted conditionally).
    plan = ["preflight"]
    if jobs:
        plan.append("jobs")
    if seed_data:
        plan.append("seed")
    if inject:
        plan.append("inject")
    plan += ["monitor", "sla-scan", "sla-check", "report", "evidence", "summary"]
    plan = [s for s in plan if s not in skip]
    total = len(plan)
    step = 0

    print(_c("1;36", "\n  QTP - Quality & Traceability Platform"))
    print(_c("36", "  Single-command compliance demonstration (ISO 9001, manufacturing)\n"))

    if not keep_journal:
        evidence_mod.reset_journal()  # clean WireMock journal so the run's actions stand out

    def begin(name, title, control):
        nonlocal step
        step += 1
        _banner(step, total, title, control)
        return name

    # 1) Preflight ---------------------------------------------------------------
    if "preflight" in plan:
        begin("preflight", "Service preflight", "environment readiness")
        results = preflight_mod.gather()
        preflight_mod.print_board(results)
        down = [r["service"] for r in results if r["required"] and not r["ok"]]
        if down:
            log.error("Required service(s) down: %s — start the stack and re-run.", ", ".join(down))
            stages.record("preflight", False, "required services down: " + ", ".join(down))
            return 1
        stages.record("preflight", True, f"{sum(r['ok'] for r in results)}/{len(results)} services up")

    # Live overlay: start Control Hub jobs --------------------------------------
    if "jobs" in plan:
        begin("jobs", "Start Control Hub jobs (live overlay)", "SDK job orchestration")
        ok, detail = start_jobs()
        log.info("%s", detail)
        stages.record("start-jobs", ok, detail, optional=True)

    # 2) Seed --------------------------------------------------------------------
    if "seed" in plan:
        begin("seed", "Seed golden-thread scenario", "deterministic, repeatable demo data")
        try:
            summary = seed_mod.seed(reset=reset)
            stages.record("seed", True,
                          f"{summary['defective']} defective / {summary['conforming']} conforming")
        except Exception as exc:  # noqa: BLE001
            log.error("Seed failed: %s", exc)
            stages.record("seed", False, str(exc))

    # Live overlay: inject a real CRITICAL into Kafka ----------------------------
    if "inject" in plan:
        begin("inject", "Inject live CRITICAL event (live overlay)", "real-time ingest path")
        ok, detail = inject_critical()
        log.info("%s", detail)
        stages.record("inject-critical", ok, detail, optional=True)

    if settle and ("seed" in plan or "inject" in plan):
        time.sleep(settle)  # let any live path settle before reading the warehouse

    # 3) Defect-rate monitor -----------------------------------------------------
    if "monitor" in plan:
        begin("monitor", "Per-line defect-rate monitor",
              "compliance: CRITICAL alert when a line exceeds 2%")
        try:
            from ..monitor.defect_rate_monitor import check

            check()
            stages.record("monitor", True, "defect-rate check complete")
        except Exception as exc:  # noqa: BLE001
            log.error("Monitor failed: %s", exc)
            stages.record("monitor", False, str(exc))

    # 4) SLA scan ----------------------------------------------------------------
    if "sla-scan" in plan:
        begin("sla-scan", "Traceability SLA scan",
              "compliance: escalate any part untraced past 2 hours")
        try:
            from ..sla.traceability_checker import scan

            scan()
            stages.record("sla-scan", True, "SLA scan complete")
        except Exception as exc:  # noqa: BLE001
            log.error("SLA scan failed: %s", exc)
            stages.record("sla-scan", False, str(exc))

    # 5) SLA chain on the hero part ---------------------------------------------
    if "sla-check" in plan:
        begin("sla-check", f"Full traceability chain · {seed_mod.HERO_PART}",
              "compliance: part -> batch -> supplier -> certificate, within SLA")
        try:
            from ..sla.traceability_checker import check_part

            check_part(seed_mod.HERO_PART)
            stages.record("sla-check", True, f"chain reconstructed for {seed_mod.HERO_PART}")
        except Exception as exc:  # noqa: BLE001
            log.error("SLA check failed: %s", exc)
            stages.record("sla-check", False, str(exc))

    # 6) Shift report ------------------------------------------------------------
    if "report" in plan:
        begin("report", f"Shift quality report · shift {shift} {date}",
              "operational: Excel -> MinIO + Mailhog")
        try:
            from ..report.shift_quality_report import generate

            rc = generate(shift, date, email or "quality.manager@kermit.local")
            stages.record("report", rc == 0, "shift report generated")
        except Exception as exc:  # noqa: BLE001
            log.error("Report failed: %s", exc)
            stages.record("report", False, str(exc))

    # 7) Evidence pack -----------------------------------------------------------
    bundle, paths = {"compliance_controls": []}, {}
    if "evidence" in plan:
        begin("evidence", "Assemble compliance evidence pack",
              "documentation: machine + human-readable audit artefacts")
        try:
            from datetime import datetime, timezone

            bundle = evidence_mod.collect(seed_mod.HERO_PART)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            target = evidence_dir or os.path.join("evidence", f"pack-{stamp}")
            paths = evidence_mod.write_pack(bundle, target)
            passed = sum(1 for c in bundle["compliance_controls"] if c["status"] == "PASS")
            log.info("Evidence pack: %s (%d/%d controls PASS).", target, passed,
                     len(bundle["compliance_controls"]))
            stages.record("evidence", True, paths["html"])
        except Exception as exc:  # noqa: BLE001
            log.error("Evidence pack failed: %s", exc)
            stages.record("evidence", False, str(exc))

    # 8) Compliance summary ------------------------------------------------------
    if "summary" in plan:
        _print_summary(bundle, paths)

    # Optional live overlays (jobs/inject) failing does not fail the run.
    return 0 if not stages.critical_failures else 1


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="demo", description="Run the QTP single-command compliance demonstration.")
    parser.add_argument("--no-seed", action="store_true", help="run against existing warehouse data")
    parser.add_argument("--no-reset", action="store_true", help="seed without wiping prior DEMO-* rows")
    parser.add_argument("--shift", default="A", choices=["A", "B", "C"], help="shift for the report")
    parser.add_argument("--date", default=date_cls.today().isoformat(), help="YYYY-MM-DD for the report")
    parser.add_argument("--email", help="quality-manager email for the report")
    parser.add_argument("--inject-critical", action="store_true",
                        help="live overlay: publish a real CRITICAL event to Kafka")
    parser.add_argument("--start-jobs", action="store_true",
                        help="live overlay: start Control Hub jobs (verify SDK job API first)")
    parser.add_argument("--keep-journal", action="store_true",
                        help="do not reset the WireMock request journal before running")
    parser.add_argument("--evidence-dir", help="write the evidence pack here")
    parser.add_argument("--skip", nargs="*", default=[],
                        help="stage names to skip (monitor sla-scan sla-check report evidence ...)")
    args = parser.parse_args(argv)
    try:
        return run(seed_data=not args.no_seed, reset=not args.no_reset, shift=args.shift,
                   date=args.date, email=args.email, inject=args.inject_critical,
                   jobs=args.start_jobs, keep_journal=args.keep_journal,
                   evidence_dir=args.evidence_dir, skip=args.skip)
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
