"""QTP command-line entrypoint: `qtp <group> <action>`.

Implemented now (Day 3): `fragments build`, `fragments list`.
The remaining groups are declared so the full command tree is visible in --help;
each is wired to a clear "planned" handler until its day in the build plan.
"""

import argparse
import os
import sys

from .common.util import get_logger
from .fragments.registry import FRAGMENT_BUILDERS, FRAGMENT_DESCRIPTIONS
from .pipelines.registry import PIPELINE_BUILDERS

log = get_logger()


# --------------------------------------------------------------------- fragments
def cmd_fragments_build(args) -> int:
    from .common.controlhub import get_authoring_engine, get_control_hub

    names = [args.name] if args.name else list(FRAGMENT_BUILDERS)
    if args.as_name and len(names) != 1:
        raise RuntimeError("--as requires --name (publish one fragment at a time under a custom name)")

    sch = get_control_hub()
    engine = get_authoring_engine(sch, args.engine_id)
    log.info("Authoring engine: %s", engine.id)

    for name in names:
        publish_name = args.as_name or name
        if args.replace:
            # `fragments build` creates a NEW fragment each run, so repeated builds leave
            # duplicates and `get(name=...)` becomes ambiguous. --replace deletes every
            # existing copy of this name first, leaving exactly one clean fragment.
            existing = list(sch.pipelines.get_all(name=publish_name, fragment=True))
            for old in existing:
                try:
                    sch.delete_pipeline(old)
                    log.info("Deleted existing fragment copy '%s'.", publish_name)
                except Exception as exc:  # noqa: BLE001
                    log.warning("Could not delete '%s' (%s); remove it manually if it lingers.",
                                publish_name, exc)
        log.info("Building fragment '%s'%s...", name, f" as '{publish_name}'" if args.as_name else "")
        fragment = FRAGMENT_BUILDERS[name](sch, engine.id, name=publish_name)
        sch.publish_pipeline(
            fragment,
            commit_message=f"qtp: publish {publish_name} v1.0",
            draft=args.draft,
        )
        log.info("Published fragment '%s'%s", publish_name, " (draft)" if args.draft else "")
    log.info("Done: %d fragment(s).", len(names))
    return 0


def cmd_fragments_list(args) -> int:
    from .common.controlhub import get_control_hub

    sch = get_control_hub()
    fragments = sch.pipelines.get_all(fragment=True)
    # OWNED = name is in our local builder registry (NOT real Control Hub ownership).
    # ID is the stable Control Hub object id — use it to pin a fragment in
    # config/fragment_pins.yaml when the name collides with a teammate's copy.
    print(f"{'FRAGMENT':40} {'OWNED':6} {'VERSION':8} ID")
    owned = set(FRAGMENT_BUILDERS)
    for frag in fragments:
        name = getattr(frag, "name", str(frag))
        fid = getattr(frag, "id", "")
        version = str(getattr(frag, "version", "") or "")
        committer = ""
        try:
            committer = frag._data.get("committer", "") or ""  # noqa: SLF001
        except Exception:  # noqa: BLE001
            pass
        flag = "yes" if name in owned else ""
        print(f"{name:40} {flag:6} {version:8} {fid}  {committer}")
    return 0


# --------------------------------------------------------------------- pipelines
def cmd_pipelines_build(args) -> int:
    from .common.controlhub import get_authoring_engine, get_control_hub

    sch = get_control_hub()
    engine = get_authoring_engine(sch, args.engine_id)
    log.info("Authoring engine: %s", engine.id)

    log.info("Building pipeline '%s'%s...", args.name, f" as '{args.as_name}'" if args.as_name else "")
    new_pipeline = PIPELINE_BUILDERS[args.name](sch, engine.id, name=args.as_name)
    full_name = getattr(new_pipeline, "name", args.as_name or args.name)
    message = args.message

    existing = list(sch.pipelines.get_all(name=full_name))
    if args.replace:
        for old in existing:
            _safe_delete(sch, old, f"existing '{full_name}'")
        existing = []

    if len(existing) > 1:
        raise RuntimeError(
            f"{len(existing)} pipelines named '{full_name}' exist - versioning is ambiguous. "
            f"Run once with --replace to consolidate, then versioning works."
        )

    if existing:
        # Version in place: transplant the freshly-built definition onto the existing
        # pipeline (which carries commit_id) so publish creates a NEW VERSION, not a dupe.
        target = existing[0]
        new_def = dict(new_pipeline._pipeline_definition)
        new_def["pipelineId"] = target._pipeline_definition.get("pipelineId", new_def.get("pipelineId"))
        new_def["title"] = target._pipeline_definition.get("title", full_name)
        target._pipeline_definition = new_def
        try:
            # The parameters setter calls .copy() on its argument, so pass a plain dict
            # (new_pipeline.parameters is a PipelineParameters Mapping, which has no .copy()).
            target.parameters = dict(new_pipeline.parameters)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not copy parameters onto existing pipeline (%s).", exc)
        sch.publish_pipeline(target, commit_message=message or f"qtp: update {full_name}")
        log.info("Published NEW VERSION of '%s' (now v%s).", full_name, getattr(target, "version", "?"))
    else:
        sch.publish_pipeline(new_pipeline, commit_message=message or f"qtp: publish {full_name} v1", draft=args.draft)
        log.info("Published '%s' (v1)%s", full_name, " (draft)" if args.draft else "")
    return 0


def _safe_delete(sch, pipeline, label):
    try:
        sch.delete_pipeline(pipeline)
        log.info("Cleaned up %s", label)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not delete %s (%s); remove it manually if it lingers.", label, exc)


def cmd_pipelines_validate(args) -> int:
    from .common.controlhub import get_authoring_engine, get_control_hub

    sch = get_control_hub()
    engine = get_authoring_engine(sch, args.engine_id)
    log.info("Authoring engine: %s", engine.id)

    # Validate needs a published pipeline, so publish a throwaway temp-named draft
    # and delete it afterwards - leaves no duplicate of the real pipeline behind.
    tmp_name = f"_tmp_validate_{args.name}"
    log.info("Publishing temp draft '%s' to validate '%s'...", tmp_name, args.name)
    pipeline = PIPELINE_BUILDERS[args.name](sch, engine.id, name=tmp_name)
    sch.publish_pipeline(pipeline, commit_message="qtp: temp validate", draft=True)
    try:
        sch.validate_pipeline(pipeline)
        log.info("VALID: '%s' passed validation (no blocking issues).", args.name)
    finally:
        _safe_delete(sch, pipeline, f"temp draft '{tmp_name}'")
    return 0


def cmd_pipelines_preview(args) -> int:
    from .common.controlhub import get_authoring_engine, get_control_hub

    sch = get_control_hub()
    engine = get_authoring_engine(sch, args.engine_id)
    log.info("Authoring engine: %s", engine.id)
    tmp_name = f"_tmp_preview_{args.name}"
    log.info("Publishing temp draft '%s' to preview '%s'...", tmp_name, args.name)
    pipeline = PIPELINE_BUILDERS[args.name](sch, engine.id, name=tmp_name)
    sch.publish_pipeline(pipeline, commit_message="qtp: temp preview", draft=True)
    log.info(
        "Previewing (skip_targets=True, %d records, up to %ds) - reads live Kafka, writes nothing...",
        args.records, args.timeout,
    )
    try:
        # wait=False so the SDK's hardcoded 30s internal wait is bypassed; we wait
        # ourselves with a real, configurable budget (cold engine + tunnelling is slow).
        preview = sch.run_pipeline_preview(
            pipeline,
            batches=1,
            batch_size=args.records,
            skip_targets=True,
            timeout=args.timeout * 1000,
            wait=False,
        )
        preview.wait_for_finished(timeout_sec=args.timeout)
        inner = getattr(preview, "preview", preview)
        issues = getattr(inner, "issues", None)
        if issues:
            log.warning("Preview issues: %s", issues)
        else:
            log.info("Preview completed with no issues - data flowed through cleanly.")
    finally:
        _safe_delete(sch, pipeline, f"temp draft '{tmp_name}'")
    return 0


# --------------------------------------------------------------------- planned groups
def make_planned(group: str, action: str, day: str):
    def _handler(args) -> int:
        log.info("`qtp %s %s` is planned for Day %s - not yet implemented.", group, action, day)
        return 0

    return _handler


_PLANNED = []  # all command groups are now implemented


# ----------------------------------------------------------------------- jobs
def cmd_jobs_status(args) -> int:
    from .jobs.job_manager import status

    return status(args.flow)


def cmd_jobs_create(args) -> int:
    from .jobs.job_manager import create

    return create(args.flow, start=args.start, wait=args.wait)


def cmd_jobs_start(args) -> int:
    from .jobs.job_manager import start

    return start(args.flow, wait=args.wait, create_missing=not args.no_create)


def cmd_jobs_stop(args) -> int:
    from .jobs.job_manager import stop

    return stop(args.flow, force=args.force)


# ------------------------------------------------------------------- teardown
def cmd_teardown_run(args) -> int:
    from .demo.teardown import run

    return run(stop_jobs=args.stop_jobs, clear_journal=args.clear_journal)


# ------------------------------------------------------------------------- demo
def cmd_demo_run(args) -> int:
    from .demo.runner import run

    return run(seed_data=not args.no_seed, reset=not args.no_reset, shift=args.shift,
               date=args.date, email=args.email, inject=args.inject_critical,
               jobs=args.start_jobs, keep_journal=args.keep_journal,
               evidence_dir=args.evidence_dir, skip=args.skip)


def cmd_demo_seed(args) -> int:
    from .demo.seed import run

    return run(reset=not args.no_reset)


# -------------------------------------------------------------------- preflight
def cmd_preflight_run(args) -> int:
    from .demo.preflight import run

    return run()


# --------------------------------------------------------------------- evidence
def cmd_evidence_pack(args) -> int:
    from .demo.evidence import pack

    return pack(args.part, args.out)


# --------------------------------------------------------------------- schedule
def cmd_schedule_run(args) -> int:
    from .demo.schedule import run

    return run(args.monitor_interval, args.sla_interval, once=args.once)


def cmd_schedule_install(args) -> int:
    from .demo.schedule import install

    return install(args.dir, args.monitor_interval, args.sla_interval)


# --------------------------------------------------------------------------- sla
def cmd_sla_check(args) -> int:
    from .sla.traceability_checker import check_part

    return check_part(args.part)


def cmd_sla_scan(args) -> int:
    from .sla.traceability_checker import scan

    return scan(args.max_alerts)


# ------------------------------------------------------------------------ report
def cmd_report_shift(args) -> int:
    from .report.shift_quality_report import generate

    return generate(args.shift, args.date, args.email)


# ------------------------------------------------------------------------ monitor
def cmd_monitor_defect_rate(args) -> int:
    from .monitor.defect_rate_monitor import check

    return check(args.window, args.threshold)


# ------------------------------------------------------- pipelines manager (per-line)
def cmd_pipelines_generate(args) -> int:
    from .manage.quality_pipeline_manager import generate

    return generate(engine_id=args.engine_id, draft=args.draft, replace=args.replace)


def cmd_pipelines_add_line(args) -> int:
    from .manage.quality_pipeline_manager import add_line

    return add_line(args.id, args.partitions, engine_id=args.engine_id, draft=args.draft)


def cmd_pipelines_promote(args) -> int:
    from .manage.quality_pipeline_manager import promote

    return promote(args.name, to_env=args.to, engine_id=args.engine_id, draft=args.draft)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qtp",
        description="Quality & Traceability Platform - StreamSets SDK automation.",
    )
    groups = parser.add_subparsers(dest="group", required=True, metavar="<group>")

    # fragments (implemented)
    fragments = groups.add_parser("fragments", help="Build, publish, and list shared fragments")
    f_actions = fragments.add_subparsers(dest="action", required=True, metavar="<action>")

    build_p = f_actions.add_parser("build", help="Build and publish owned fragments to Control Hub")
    build_p.add_argument("--name", choices=sorted(FRAGMENT_BUILDERS), help="build only this fragment")
    build_p.add_argument("--engine-id", help="authoring Data Collector id (else ENGINE_ID from .env)")
    build_p.add_argument("--draft", action="store_true", help="publish as draft (no version bump)")
    build_p.add_argument("--as", dest="as_name", metavar="NAME",
                         help="publish under a different name (temp name for draft verification; requires --name)")
    build_p.set_defaults(func=cmd_fragments_build)

    list_p = f_actions.add_parser("list", help="List fragments in the workspace")
    list_p.set_defaults(func=cmd_fragments_list)

    # pipelines (build implemented)
    pipelines = groups.add_parser("pipelines", help="Build and publish manufacturing pipelines")
    p_actions = pipelines.add_subparsers(dest="action", required=True, metavar="<action>")
    p_build = p_actions.add_parser("build", help="Build and publish a pipeline to Control Hub")
    p_build.add_argument("--name", choices=sorted(PIPELINE_BUILDERS), required=True, help="pipeline to build")
    p_build.add_argument("--engine-id", help="authoring Data Collector id (else ENGINE_ID/SDC_ID from .env)")
    p_build.add_argument("--draft", action="store_true", help="publish v1 as draft (create path only)")
    p_build.add_argument("--replace", action="store_true", help="delete existing same-named pipeline(s) and recreate v1")
    p_build.add_argument("-m", "--message", help="commit message for this version")
    p_build.add_argument("--as", dest="as_name", metavar="NAME", help="publish under a different name")
    p_build.set_defaults(func=cmd_pipelines_build)

    p_validate = p_actions.add_parser("validate", help="Validate a pipeline against the engine (SDK)")
    p_validate.add_argument("--name", choices=sorted(PIPELINE_BUILDERS), required=True)
    p_validate.add_argument("--engine-id", help="authoring Data Collector id (else ENGINE_ID/SDC_ID)")
    p_validate.set_defaults(func=cmd_pipelines_validate)

    p_preview = p_actions.add_parser("preview", help="Preview a pipeline (reads live data, writes nothing)")
    p_preview.add_argument("--name", choices=sorted(PIPELINE_BUILDERS), required=True)
    p_preview.add_argument("--records", type=int, default=10, help="records to pull (default 10)")
    p_preview.add_argument("--timeout", type=int, default=180, help="max seconds to wait (default 180)")
    p_preview.add_argument("--engine-id", help="authoring Data Collector id (else ENGINE_ID/SDC_ID)")
    p_preview.set_defaults(func=cmd_pipelines_preview)

    # per-line fleet management (implemented)
    p_gen = p_actions.add_parser("generate", help="generate per-line ingestion + defect pipelines from lines.yaml")
    p_gen.add_argument("--engine-id", help="authoring Data Collector id (else ENGINE_ID/SDC_ID)")
    p_gen.add_argument("--draft", action="store_true", help="publish as drafts")
    p_gen.add_argument("--replace", action="store_true", help="replace existing same-named pipelines")
    p_gen.set_defaults(func=cmd_pipelines_generate)

    p_add = p_actions.add_parser("add-line", help="onboard a new production line (updates lines.yaml + generates)")
    p_add.add_argument("--id", required=True, help="new line id, e.g. line_4")
    p_add.add_argument("--partitions", default="0", help="Kafka partition(s) for the line")
    p_add.add_argument("--engine-id", help="authoring Data Collector id (else ENGINE_ID/SDC_ID)")
    p_add.add_argument("--draft", action="store_true", help="publish as drafts")
    p_add.set_defaults(func=cmd_pipelines_add_line)

    p_promote = p_actions.add_parser("promote", help="promote a pipeline to another env (env_map pattern)")
    p_promote.add_argument("--name", required=True, help="pipeline name to promote")
    p_promote.add_argument("--to", default="prod", help="target environment (default prod)")
    p_promote.add_argument("--engine-id", help="authoring Data Collector id (else ENGINE_ID/SDC_ID)")
    p_promote.add_argument("--draft", action="store_true", help="publish the promoted pipeline as a draft")
    p_promote.set_defaults(func=cmd_pipelines_promote)

    act = p_actions.add_parser("deploy", help="(planned, Day 6)")
    act.set_defaults(func=make_planned("pipelines", "deploy", "6"))

    # sla (implemented) — 2-hour ISO 9001 traceability compliance
    sla = groups.add_parser("sla", help="Traceability 2-hour SLA enforcement (ISO 9001)")
    sla_actions = sla.add_subparsers(dest="action", required=True, metavar="<action>")
    s_check = sla_actions.add_parser("check", help="full traceability + SLA report for one part")
    s_check.add_argument("--part", required=True, help="part_id to check")
    s_check.set_defaults(func=cmd_sla_check)
    s_scan = sla_actions.add_parser("scan", help="poll for SLA breaches and raise alerts")
    s_scan.add_argument("--max-alerts", type=int, default=10, help="cap alerts per scan (default 10)")
    s_scan.set_defaults(func=cmd_sla_scan)

    # monitor (implemented) — operational compliance checks
    monitor = groups.add_parser("monitor", help="Operational monitoring / compliance checks")
    monitor_actions = monitor.add_subparsers(dest="action", required=True, metavar="<action>")
    m_dr = monitor_actions.add_parser("defect-rate", help="per-line defect-rate check (CRITICAL alert if >2%)")
    m_dr.add_argument("--window", type=int, default=100, help="rolling window of recent parts per line (default 100)")
    m_dr.add_argument("--threshold", type=float, default=2.0, help="defect-rate %% ceiling (default 2.0)")
    m_dr.set_defaults(func=cmd_monitor_defect_rate)

    # report (implemented) — end-of-shift quality report
    import datetime as _dt
    report = groups.add_parser("report", help="Operational quality reports")
    report_actions = report.add_subparsers(dest="action", required=True, metavar="<action>")
    r_shift = report_actions.add_parser("shift", help="end-of-shift quality report (Excel -> MinIO + Mailhog)")
    r_shift.add_argument("--shift", required=True, choices=["A", "B", "C"], help="production shift")
    r_shift.add_argument("--date", default=_dt.date.today().isoformat(), help="YYYY-MM-DD (default: today)")
    r_shift.add_argument("--email", default="quality.manager@kermit.local", help="quality manager email")
    r_shift.set_defaults(func=cmd_report_shift)

    # demo (implemented) — single-command end-to-end compliance demonstration
    demo = groups.add_parser("demo", help="One-command end-to-end compliance demonstration")
    demo_actions = demo.add_subparsers(dest="action", required=True, metavar="<action>")
    d_run = demo_actions.add_parser("run", help="preflight -> seed -> compliance suite -> evidence -> scorecard")
    d_run.add_argument("--no-seed", action="store_true", help="run against existing warehouse data")
    d_run.add_argument("--no-reset", action="store_true", help="seed without wiping prior DEMO-* rows")
    d_run.add_argument("--shift", default="A", choices=["A", "B", "C"], help="shift for the report")
    d_run.add_argument("--date", default=_dt.date.today().isoformat(), help="YYYY-MM-DD for the report")
    d_run.add_argument("--email", help="quality-manager email for the report")
    d_run.add_argument("--inject-critical", action="store_true",
                       help="live overlay: publish a real CRITICAL event to Kafka")
    d_run.add_argument("--start-jobs", action="store_true",
                       help="live overlay: start Control Hub jobs (verify SDK job API first)")
    d_run.add_argument("--keep-journal", action="store_true",
                       help="do not reset the WireMock request journal before running")
    d_run.add_argument("--evidence-dir", help="write the evidence pack here")
    d_run.add_argument("--skip", nargs="*", default=[],
                       help="stage names to skip (monitor sla-scan sla-check report evidence ...)")
    d_run.set_defaults(func=cmd_demo_run)
    d_seed = demo_actions.add_parser("seed", help="seed the deterministic golden-thread scenario")
    d_seed.add_argument("--no-reset", action="store_true", help="upsert without wiping prior DEMO-* rows")
    d_seed.set_defaults(func=cmd_demo_seed)

    # preflight (implemented) — service health board
    preflight = groups.add_parser("preflight", help="Service health board (run before a demo)")
    pf_actions = preflight.add_subparsers(dest="action", required=True, metavar="<action>")
    pf_run = pf_actions.add_parser("run", help="probe every service and print GREEN/RED board")
    pf_run.set_defaults(func=cmd_preflight_run)

    # evidence (implemented) — compliance evidence pack (JSON + HTML)
    evidence = groups.add_parser("evidence", help="Assemble the compliance evidence pack")
    ev_actions = evidence.add_subparsers(dest="action", required=True, metavar="<action>")
    ev_pack = ev_actions.add_parser("pack", help="aggregate warehouse + WireMock journal -> JSON + HTML")
    ev_pack.add_argument("--part", help="hero part_id for the traceability chain (default: demo hero)")
    ev_pack.add_argument("--out", help="output directory (default: evidence/pack-<timestamp>)")
    ev_pack.set_defaults(func=cmd_evidence_pack)

    # schedule (implemented) — recurring compliance cadence
    schedule = groups.add_parser("schedule", help="Run or install the recurring compliance cadence")
    sc_actions = schedule.add_subparsers(dest="action", required=True, metavar="<action>")
    sc_run = sc_actions.add_parser("run", help="run the cadence in this process (monitor + SLA + report)")
    sc_run.add_argument("--monitor-interval", type=int, default=5, help="defect-rate interval, minutes")
    sc_run.add_argument("--sla-interval", type=int, default=15, help="SLA scan interval, minutes")
    sc_run.add_argument("--once", action="store_true", help="run each poller once and exit")
    sc_run.set_defaults(func=cmd_schedule_run)
    sc_install = sc_actions.add_parser("install", help="write cron / Task Scheduler / compose artefacts")
    sc_install.add_argument("--dir", default="scripts/scheduled", help="output directory")
    sc_install.add_argument("--monitor-interval", type=int, default=5)
    sc_install.add_argument("--sla-interval", type=int, default=15)
    sc_install.set_defaults(func=cmd_schedule_install)

    # jobs (implemented) — Control Hub job lifecycle for the live path
    jobs = groups.add_parser("jobs", help="Control Hub job lifecycle (status/create/start/stop)")
    j_actions = jobs.add_subparsers(dest="action", required=True, metavar="<action>")
    j_status = j_actions.add_parser("status", help="status board for the manufacturing jobs")
    j_status.add_argument("--flow", nargs="*", help="flow names (default: quality_ingest defect_enrichment)")
    j_status.set_defaults(func=cmd_jobs_status)
    j_create = j_actions.add_parser("create", help="create missing jobs from their published pipelines")
    j_create.add_argument("--flow", nargs="*", help="flow names to create")
    j_create.add_argument("--start", action="store_true", help="start the jobs after creating them")
    j_create.add_argument("--wait", action="store_true", help="wait for RUNNING before returning")
    j_create.set_defaults(func=cmd_jobs_create)
    j_start = j_actions.add_parser("start", help="start the core jobs (creates them if missing)")
    j_start.add_argument("--flow", nargs="*", help="flow names to start")
    j_start.add_argument("--wait", action="store_true", help="wait for RUNNING before returning")
    j_start.add_argument("--no-create", action="store_true", help="do not auto-create missing jobs")
    j_start.set_defaults(func=cmd_jobs_start)
    j_stop = j_actions.add_parser("stop", help="stop the core jobs")
    j_stop.add_argument("--flow", nargs="*", help="flow names to stop")
    j_stop.add_argument("--force", action="store_true", help="force-stop without graceful wait")
    j_stop.set_defaults(func=cmd_jobs_stop)

    # teardown (implemented) — remove the demo's DEMO-* footprint
    teardown = groups.add_parser("teardown", help="Remove DEMO-* data (and optionally stop jobs)")
    t_actions = teardown.add_subparsers(dest="action", required=True, metavar="<action>")
    t_run = t_actions.add_parser("run", help="delete DEMO-* rows; real warehouse data untouched")
    t_run.add_argument("--stop-jobs", action="store_true", help="also stop Control Hub jobs")
    t_run.add_argument("--clear-journal", action="store_true", help="also clear the WireMock journal")
    t_run.set_defaults(func=cmd_teardown_run)

    # planned groups (declared for the full command tree)
    for group, actions, day in _PLANNED:
        grp = groups.add_parser(group, help=f"(planned, Day {day})")
        grp_actions = grp.add_subparsers(dest="action", required=True, metavar="<action>")
        for action in actions:
            act = grp_actions.add_parser(action, help=f"(planned, Day {day})")
            act.set_defaults(func=make_planned(group, action, day))

    return parser


def main(argv=None) -> int:
    # Prefer UTF-8 console output so log messages (em dashes, section signs) render cleanly
    # on Windows terminals instead of mojibake. Best-effort: harmless if unsupported.
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass
    # Cap BLAS thread pools (numpy is pulled in transitively by openpyxl in the report).
    # The CLI does no real linear algebra, and a multi-thread BLAS pool can fail to allocate
    # on a low-RAM host running the full Docker stack (OpenBLAS "Memory allocation failed").
    # Must be set before numpy is first imported; respects any value the user already set.
    for _var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ.setdefault(_var, "1")
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except Exception as exc:  # noqa: BLE001 - surface a clean message, not a traceback
        log.error("%s", exc)
        # Set QTP_DEBUG=1 to see the full traceback (which file/line actually failed).
        if os.environ.get("QTP_DEBUG"):
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
