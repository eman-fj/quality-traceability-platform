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
    print(f"{'FRAGMENT':40} OWNED")
    owned = set(FRAGMENT_BUILDERS)
    for frag in fragments:
        name = getattr(frag, "name", str(frag))
        print(f"{name:40} {'yes' if name in owned else ''}")
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


_PLANNED = [
    ("jobs", ["status", "start", "stop"], "6"),
    ("preflight", ["run"], "7"),
    ("evidence", ["pack"], "8"),
    ("demo", ["run"], "8"),
    ("teardown", ["run"], "9"),
]


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

    for action, day in [("generate", "4-6"), ("add-line", "6"), ("promote", "6"), ("deploy", "6")]:
        act = p_actions.add_parser(action, help=f"(planned, Day {day})")
        act.set_defaults(func=make_planned("pipelines", action, day))

    # sla (implemented) — 2-hour ISO 9001 traceability compliance
    sla = groups.add_parser("sla", help="Traceability 2-hour SLA enforcement (ISO 9001)")
    sla_actions = sla.add_subparsers(dest="action", required=True, metavar="<action>")
    s_check = sla_actions.add_parser("check", help="full traceability + SLA report for one part")
    s_check.add_argument("--part", required=True, help="part_id to check")
    s_check.set_defaults(func=cmd_sla_check)
    s_scan = sla_actions.add_parser("scan", help="poll for SLA breaches and raise alerts")
    s_scan.add_argument("--max-alerts", type=int, default=10, help="cap alerts per scan (default 10)")
    s_scan.set_defaults(func=cmd_sla_scan)

    # report (implemented) — end-of-shift quality report
    import datetime as _dt
    report = groups.add_parser("report", help="Operational quality reports")
    report_actions = report.add_subparsers(dest="action", required=True, metavar="<action>")
    r_shift = report_actions.add_parser("shift", help="end-of-shift quality report (Excel -> MinIO + Mailhog)")
    r_shift.add_argument("--shift", required=True, choices=["A", "B", "C"], help="production shift")
    r_shift.add_argument("--date", default=_dt.date.today().isoformat(), help="YYYY-MM-DD (default: today)")
    r_shift.add_argument("--email", default="quality.manager@kermit.local", help="quality manager email")
    r_shift.set_defaults(func=cmd_report_shift)

    # planned groups (declared for the full command tree)
    for group, actions, day in _PLANNED:
        grp = groups.add_parser(group, help=f"(planned, Day {day})")
        grp_actions = grp.add_subparsers(dest="action", required=True, metavar="<action>")
        for action in actions:
            act = grp_actions.add_parser(action, help=f"(planned, Day {day})")
            act.set_defaults(func=make_planned(group, action, day))

    return parser


def main(argv=None) -> int:
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
