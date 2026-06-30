"""schedule.py — run QTP's compliance cadence on a schedule.

The brief's compliance controls are not one-shots — they are recurring duties:

  * defect-rate monitor — short cadence (default every 5 min),
  * traceability SLA scan — every 15 min (the brief's explicit interval),
  * shift quality report — at each shift boundary (06:00 / 14:00 / 22:00).

`qtp schedule run` runs all three in a single long-lived process using only the standard
library (no extra dependency, cross-platform) — this is what the optional `qtp-scheduler`
docker service runs. `qtp schedule run --once` fires each poller a single time (handy for a
cron/Task-Scheduler entry or a quick local check).

`qtp schedule install` writes ready-to-use scheduler artefacts (crontab, Windows Task
Scheduler commands, and a docker-compose service) so the cadence can be installed the native
way on any host — nothing hardcoded, all derived from the configured intervals.
"""

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..common.util import get_logger

log = get_logger()

DEFAULT_MONITOR_MIN = 5
DEFAULT_SLA_MIN = 15
SHIFT_BOUNDARIES = ["06:00", "14:00", "22:00"]


# ----------------------------------------------------------------------- tasks
def _run_monitor() -> None:
    from ..monitor.defect_rate_monitor import check

    log.info("[schedule] defect-rate monitor")
    check()


def _run_sla_scan() -> None:
    from ..sla.traceability_checker import scan

    log.info("[schedule] traceability SLA scan")
    scan()


def ended_shift(now: datetime) -> tuple[str, str]:
    """The shift (and its date) that has most recently ended at a boundary.

    Boundaries: A 06-14, B 14-22, C 22-06. At 14:00 shift A just ended (today); at 22:00
    shift B (today); at 06:00 shift C (which began the previous calendar day).
    """
    h = now.hour
    if h >= 22:
        return "B", now.date().isoformat()
    if h >= 14:
        return "A", now.date().isoformat()
    if h >= 6:
        return "C", (now.date() - timedelta(days=1)).isoformat()
    return "C", (now.date() - timedelta(days=1)).isoformat()


def _run_report() -> None:
    from ..report.shift_quality_report import generate

    shift, day = ended_shift(datetime.now(timezone.utc))
    log.info("[schedule] shift report for shift %s %s", shift, day)
    generate(shift, day, "quality.manager@kermit.local")


# ----------------------------------------------------------------------- runner
def _next_boundary(now: datetime, boundaries: list[str]) -> datetime:
    """Next datetime at one of the HH:MM boundaries (today or tomorrow)."""
    today = now.date()
    candidates = []
    for hhmm in boundaries:
        hh, mm = (int(x) for x in hhmm.split(":"))
        dt = datetime(today.year, today.month, today.day, hh, mm, tzinfo=now.tzinfo)
        if dt <= now:
            dt += timedelta(days=1)
        candidates.append(dt)
    return min(candidates)


def run(monitor_min: int = DEFAULT_MONITOR_MIN, sla_min: int = DEFAULT_SLA_MIN,
        boundaries: list[str] | None = None, once: bool = False) -> int:
    """Run the compliance cadence. ``once`` fires each poller once and returns."""
    boundaries = boundaries or SHIFT_BOUNDARIES

    if once:
        log.info("Running each compliance task once (--once).")
        for fn in (_run_monitor, _run_sla_scan):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001 - one task must not kill the run
                log.error("Task error: %s", exc)
        return 0

    now = datetime.now(timezone.utc)
    schedule = [
        {"name": "defect-rate monitor", "fn": _run_monitor,
         "next": now, "every": timedelta(minutes=monitor_min)},
        {"name": "SLA scan", "fn": _run_sla_scan,
         "next": now, "every": timedelta(minutes=sla_min)},
        {"name": "shift report", "fn": _run_report,
         "next": _next_boundary(now, boundaries), "every": None, "boundaries": boundaries},
    ]
    log.info("Scheduler started: monitor every %dm, SLA scan every %dm, report at %s. "
             "Ctrl-C to stop.", monitor_min, sla_min, ",".join(boundaries))

    try:
        while True:
            now = datetime.now(timezone.utc)
            for task in schedule:
                if now >= task["next"]:
                    try:
                        task["fn"]()
                    except Exception as exc:  # noqa: BLE001
                        log.error("Task '%s' error: %s", task["name"], exc)
                    if task["every"] is not None:
                        task["next"] = now + task["every"]
                    else:
                        task["next"] = _next_boundary(now, task["boundaries"])
                    log.info("[schedule] next '%s' at %s", task["name"],
                             task["next"].strftime("%Y-%m-%d %H:%M:%S %Z"))
            time.sleep(5)
    except KeyboardInterrupt:
        log.info("Scheduler stopped.")
        return 0


# --------------------------------------------------------------------- install
def render_artifacts(monitor_min: int, sla_min: int, boundaries: list[str]) -> dict:
    """Render cron / Windows Task Scheduler / docker-compose scheduler artefacts."""
    repo = "/path/to/quality-traceability-platform"
    cron = "\n".join([
        "# QTP compliance cadence — install with: crontab qtp-cron",
        f"*/{monitor_min} * * * * cd {repo} && qtp monitor defect-rate >> /var/log/qtp-monitor.log 2>&1",
        f"*/{sla_min} * * * * cd {repo} && qtp sla scan >> /var/log/qtp-sla.log 2>&1",
        "0 14 * * * cd %s && qtp report shift --shift A >> /var/log/qtp-report.log 2>&1" % repo,
        "0 22 * * * cd %s && qtp report shift --shift B >> /var/log/qtp-report.log 2>&1" % repo,
        "0 6  * * * cd %s && qtp report shift --shift C >> /var/log/qtp-report.log 2>&1" % repo,
        "",
    ])

    schtasks = "\n".join([
        "# QTP compliance cadence — Windows Task Scheduler (run in an elevated PowerShell).",
        "# Adjust -TR working dir to your repo path; assumes `qtp` is on PATH.",
        f'schtasks /Create /SC MINUTE /MO {monitor_min} /TN "QTP\\DefectRate" '
        '/TR "cmd /c qtp monitor defect-rate" /F',
        f'schtasks /Create /SC MINUTE /MO {sla_min} /TN "QTP\\SlaScan" '
        '/TR "cmd /c qtp sla scan" /F',
        'schtasks /Create /SC DAILY /ST 14:00 /TN "QTP\\ReportA" '
        '/TR "cmd /c qtp report shift --shift A" /F',
        'schtasks /Create /SC DAILY /ST 22:00 /TN "QTP\\ReportB" '
        '/TR "cmd /c qtp report shift --shift B" /F',
        'schtasks /Create /SC DAILY /ST 06:00 /TN "QTP\\ReportC" '
        '/TR "cmd /c qtp report shift --shift C" /F',
        "",
    ])

    compose = "\n".join([
        "# Append to docker-compose.yml (or use `docker compose -f docker-compose.scheduler.yml up`)",
        "# to run the cadence as a service inside the stack.",
        "  qtp-scheduler:",
        "    build: .",
        "    container_name: qtp-scheduler",
        "    command: [\"qtp\", \"schedule\", \"run\",",
        f"             \"--monitor-interval\", \"{monitor_min}\", \"--sla-interval\", \"{sla_min}\"]",
        "    env_file: [.env]",
        "    environment:",
        "      PG_HOST: postgres",
        "      MYSQL_HOST: mysql",
        "      WIREMOCK_BASE_URL: http://wiremock:8080",
        "      MAILHOG_HOST: mailhog",
        "    depends_on: [postgres, wiremock]",
        "    restart: unless-stopped",
        "    networks: [qtp]",
        "",
    ])
    return {"qtp-cron": cron, "qtp-schedule-windows.txt": schtasks,
            "qtp-scheduler.compose.yml": compose}


def install(out_dir: str = "scripts/scheduled", monitor_min: int = DEFAULT_MONITOR_MIN,
            sla_min: int = DEFAULT_SLA_MIN, boundaries: list[str] | None = None) -> int:
    boundaries = boundaries or SHIFT_BOUNDARIES
    artifacts = render_artifacts(monitor_min, sla_min, boundaries)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, content in artifacts.items():
        (out / name).write_text(content, encoding="utf-8")
    log.info("Wrote %d scheduler artefact(s) to %s", len(artifacts), out)
    print(f"\n  Scheduler artefacts written to {out}/:")
    for name in artifacts:
        print(f"    - {name}")
    print("\n  Native install options:")
    print("    Linux/macOS : crontab scripts/scheduled/qtp-cron")
    print("    Windows     : run the commands in scripts/scheduled/qtp-schedule-windows.txt")
    print("    Docker      : add scripts/scheduled/qtp-scheduler.compose.yml to your stack")
    print("    In-process  : qtp schedule run   (no OS scheduler needed)\n")
    return 0


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="schedule",
                                     description="Run or install the QTP compliance cadence.")
    sub = parser.add_subparsers(dest="action", required=True, metavar="<action>")

    p_run = sub.add_parser("run", help="run the cadence in this process")
    p_run.add_argument("--monitor-interval", type=int, default=DEFAULT_MONITOR_MIN,
                       help=f"defect-rate monitor interval, minutes (default {DEFAULT_MONITOR_MIN})")
    p_run.add_argument("--sla-interval", type=int, default=DEFAULT_SLA_MIN,
                       help=f"SLA scan interval, minutes (default {DEFAULT_SLA_MIN})")
    p_run.add_argument("--once", action="store_true", help="run each poller once and exit")

    p_install = sub.add_parser("install", help="write cron / Task Scheduler / compose artefacts")
    p_install.add_argument("--dir", default="scripts/scheduled", help="output directory")
    p_install.add_argument("--monitor-interval", type=int, default=DEFAULT_MONITOR_MIN)
    p_install.add_argument("--sla-interval", type=int, default=DEFAULT_SLA_MIN)

    args = parser.parse_args(argv)
    try:
        if args.action == "run":
            return run(args.monitor_interval, args.sla_interval, once=args.once)
        return install(args.dir, args.monitor_interval, args.sla_interval)
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
