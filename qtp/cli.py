"""QTP command-line entrypoint: `qtp <group> <action>`.

Implemented now (Day 3): `fragments build`, `fragments list`.
The remaining groups are declared so the full command tree is visible in --help;
each is wired to a clear "planned" handler until its day in the build plan.
"""

import argparse
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

    publish_name = args.as_name or args.name
    log.info("Building pipeline '%s'%s...", args.name, f" as '{publish_name}'" if args.as_name else "")
    pipeline = PIPELINE_BUILDERS[args.name](sch, engine.id, name=args.as_name)
    sch.publish_pipeline(
        pipeline,
        commit_message=f"qtp: publish {publish_name}",
        draft=args.draft,
    )
    log.info("Published pipeline '%s'%s", publish_name, " (draft)" if args.draft else "")
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
    ("sla", ["check"], "7"),
    ("report", ["shift"], "8"),
    ("evidence", ["pack"], "8"),
    ("demo", ["run"], "8"),
    ("teardown", ["run"], "9"),
]


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
    p_build.add_argument("--draft", action="store_true", help="publish as draft")
    p_build.add_argument("--as", dest="as_name", metavar="NAME", help="publish under a different name")
    p_build.set_defaults(func=cmd_pipelines_build)
    for action, day in [("generate", "4-6"), ("add-line", "6"), ("promote", "6"), ("deploy", "6")]:
        act = p_actions.add_parser(action, help=f"(planned, Day {day})")
        act.set_defaults(func=make_planned("pipelines", action, day))

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
        return 1


if __name__ == "__main__":
    sys.exit(main())
