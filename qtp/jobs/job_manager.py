"""job_manager.py — Control Hub job lifecycle (status / create / start / stop).

Maps each core manufacturing flow to a Control Hub job following the workspace naming
convention (brief §9.1):

    pipeline  manufacturing__<flow>           ->  job  manufacturing__job__<flow>

and drives it through the SDK: ``sch.get_job_builder().build(...)`` + ``sch.add_job(...)``
to create, ``sch.start_job(...)`` / ``sch.stop_job(...)`` to run, and ``job.status`` to
report. Jobs are tagged ``industry:manufacturing`` / ``env:dev`` for workspace navigability.

Nothing is hardcoded beyond the naming convention; Control Hub credentials come from .env
via ``qtp.common.controlhub``. These commands change live Control Hub state, so they are run
by the engineer against a reachable engine (the assistant only authors/verifies the code).
"""

from ..common.controlhub import get_control_hub
from ..common.util import get_logger

log = get_logger()

# Core flows the live demo needs running. Extra flows can be added via --flow on the CLI.
DEFAULT_FLOWS = ["quality_ingest", "defect_enrichment"]

PIPELINE_PREFIX = "manufacturing__"
JOB_PREFIX = "manufacturing__job__"
JOB_TAGS = ["industry:manufacturing", "env:dev"]


def pipeline_name(flow: str) -> str:
    return PIPELINE_PREFIX + flow


def job_name(flow: str) -> str:
    return JOB_PREFIX + flow


def _status_str(job) -> str:
    """Human-readable status for a Job (handles never-started jobs whose status is None)."""
    try:
        st = job.status
    except Exception:  # noqa: BLE001
        return "UNKNOWN"
    if st is None:
        return "INACTIVE (never run)"
    return getattr(st, "status", None) or "UNKNOWN"


def _find_job(sch, name: str):
    """Return the job named ``name`` or None (offline name filter; tolerant of SDK quirks)."""
    try:
        return sch.jobs.get(job_name=name)
    except Exception:  # noqa: BLE001 - not found / ambiguous -> fall back to a scan
        for job in sch.jobs:
            if getattr(job, "job_name", None) == name:
                return job
    return None


def _resolve_pipeline(sch, name: str):
    """Resolve a single published pipeline by name, tolerating duplicate-name workspaces."""
    matches = list(sch.pipelines.get_all(name=name))
    if not matches:
        raise RuntimeError(
            f"Pipeline '{name}' not found in the workspace — build/publish it first "
            f"(qtp pipelines build --name {name.replace(PIPELINE_PREFIX, '')})."
        )
    if len(matches) > 1:
        log.warning("%d pipelines named '%s'; using the most recent.", len(matches), name)
    return matches[-1]


# --------------------------------------------------------------------------- actions
def status(flows=None) -> int:
    """Print a status board for the manufacturing jobs."""
    sch = get_control_hub()
    names = [job_name(f) for f in (flows or DEFAULT_FLOWS)]

    print(f"\n  {'JOB':40} {'STATUS':22} ID")
    print("  " + "-" * 78)
    for name in names:
        job = _find_job(sch, name)
        if job is None:
            print(f"  {name:40} {'(not created)':22} -")
        else:
            print(f"  {name:40} {_status_str(job):22} {getattr(job, 'id', '')}")
    print("  " + "-" * 78 + "\n")
    return 0


def create(flows=None, start=False, wait=False) -> int:
    """Create any missing jobs from their published pipelines (idempotent)."""
    sch = get_control_hub()
    created, existing = [], []
    for flow in (flows or DEFAULT_FLOWS):
        jname = job_name(flow)
        if _find_job(sch, jname) is not None:
            existing.append(jname)
            log.info("Job '%s' already exists.", jname)
            continue
        pipeline = _resolve_pipeline(sch, pipeline_name(flow))
        builder = sch.get_job_builder()
        job = builder.build(jname, pipeline, tags=list(JOB_TAGS))
        sch.add_job(job)
        created.append(jname)
        log.info("Created job '%s' from pipeline '%s'.", jname, pipeline_name(flow))

    log.info("Jobs ready: %d created, %d already existed.", len(created), len(existing))
    if start:
        return _start(sch, flows or DEFAULT_FLOWS, wait=wait)
    return 0


def _is_active(job) -> bool:
    """True if the job is already running (any ACTIVE/ACTIVATING state)."""
    return _status_str(job).upper().startswith("ACTIV")


def _start(sch, flows, wait=False) -> int:
    to_start, already = [], []
    for flow in flows:
        job = _find_job(sch, job_name(flow))
        if job is None:
            log.warning("Job '%s' not found — run `qtp jobs create` first.", job_name(flow))
            continue
        (already if _is_active(job) else to_start).append(job)

    if already:
        log.info("Already running: %s", ", ".join(j.job_name for j in already))
    if not to_start:
        if already:
            return 0  # everything is already up — nothing to do (idempotent, not an error)
        log.error("No jobs to start.")
        return 1
    sch.start_job(*to_start, wait=wait)
    log.info("Started %d job(s): %s%s", len(to_start), ", ".join(j.job_name for j in to_start),
             "" if wait else " (not waiting for RUNNING)")
    return 0


def start(flows=None, wait=False, create_missing=True) -> int:
    """Start the core jobs, creating any that don't exist yet."""
    sch = get_control_hub()
    flows = flows or DEFAULT_FLOWS
    if create_missing:
        for flow in flows:
            if _find_job(sch, job_name(flow)) is None:
                pipeline = _resolve_pipeline(sch, pipeline_name(flow))
                job = sch.get_job_builder().build(job_name(flow), pipeline, tags=list(JOB_TAGS))
                sch.add_job(job)
                log.info("Created missing job '%s'.", job_name(flow))
    return _start(sch, flows, wait=wait)


def stop(flows=None, force=False) -> int:
    """Stop the core jobs that are currently running."""
    sch = get_control_hub()
    jobs = []
    for flow in (flows or DEFAULT_FLOWS):
        job = _find_job(sch, job_name(flow))
        if job is not None:
            jobs.append(job)
    if not jobs:
        log.info("No matching jobs to stop.")
        return 0
    sch.stop_job(*jobs, force=force)
    log.info("Stopped %d job(s): %s", len(jobs), ", ".join(j.job_name for j in jobs))
    return 0
