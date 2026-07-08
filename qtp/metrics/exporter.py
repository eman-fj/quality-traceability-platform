"""exporter.py — `qtp metrics serve`, a Prometheus exporter for real pipeline-health metrics.

Exposes, in Prometheus text format:

    qtp_job_up{job_name}                          1 if the job's current status is ACTIVE
    qtp_job_input_records{job_name}                input record count, latest completed run
    qtp_job_output_records{job_name}               output record count, latest completed run
    qtp_job_error_records{job_name}                total error record count, latest run
    qtp_job_error_rate{job_name}                   error / input for the latest run (0..1)
    qtp_job_last_run_duration_seconds{job_name}    finish_time - start_time of the latest run
    qtp_alerts_fired_total{severity}               alerts posted (WireMock journal), by severity
    qtp_jira_tickets_created_total                 Jira tickets created (WireMock journal)

These are REAL StreamSets job metrics pulled through the Control Hub SDK session (the same
SCH_URL/SCH_CRED_ID/SCH_TOKEN every other qtp command already authenticates with) — not a
proxy computed from the warehouse. The engine's own local /metrics REST API cannot be scraped
directly: it is registered with Control Hub via DPM (`dpm.enabled=true` in the engine's
sdc.properties), so unauthenticated requests to it redirect to a login page and there is no
local username/password to supply. `Job.metrics` / `Job.history` give the same per-run record
counts and timings the Control Hub UI's own job-metrics view shows, reached through the
Control Hub API instead of the engine's local one — this is the "properly connected" path
that doesn't need any extra engine-local credentials.

`qtp metrics serve` runs indefinitely (the `qtp-metrics` docker-compose service Prometheus
scrapes). `qtp metrics serve --once` prints one snapshot and exits — a quick health check.
"""

import time

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.exposition import start_http_server

from ..common.util import get_logger
from ..jobs.job_manager import DEFAULT_FLOWS, _find_job, job_name

log = get_logger()

DEFAULT_PORT = 9091


def _latest_run_metrics(job):
    """The latest run's JobMetrics, or None if the job has never run."""
    try:
        runs = job.metrics
    except Exception as exc:  # noqa: BLE001 - a job with no runs yet, or a transient API error
        log.debug("No metrics for job '%s': %s", job.job_name, exc)
        return None
    return runs[0] if runs else None


def _latest_run_duration_seconds(job):
    """Seconds between start_time and finish_time of the most recent completed run, if known.

    Both are raw millisecond epoch integers on JobStatus (confirmed live: subtracting them
    as datetimes raised AttributeError) — matching how Job.start_time itself converts the
    same raw field (`self._data['currentJobStatus']['startTime'] / 1000`).
    """
    try:
        history = job.history
    except Exception as exc:  # noqa: BLE001
        log.debug("No history for job '%s': %s", job.job_name, exc)
        return None
    if not history:
        return None
    latest = history[0]
    start = getattr(latest, "start_time", None)
    finish = getattr(latest, "finish_time", None)
    # A currently-RUNNING job has no finish_time yet (reported as 0/None), which previously
    # produced (0 - start)/1000 ≈ -56 years. Only report a duration for a COMPLETED run:
    # both timestamps present, non-zero, and finish after start.
    if not start or not finish:
        return None
    try:
        if finish <= start:
            return None
        return (finish - start) / 1000.0
    except TypeError:  # noqa: BLE001 - unexpected timestamp shape; don't crash the scrape
        return None


def _is_active(job) -> bool:
    try:
        status = job.status
    except Exception:  # noqa: BLE001
        return False
    return bool(status) and str(getattr(status, "status", "")).upper().startswith("ACTIV")


class QtpCollector:
    """A prometheus_client Collector: computes fresh values on every scrape (pull-based),
    rather than caching gauges between scrapes — avoids ever serving stale numbers."""

    def __init__(self, sch, flows=None):
        self.sch = sch
        self.flows = flows or DEFAULT_FLOWS

    def collect(self):
        up = GaugeMetricFamily("qtp_job_up", "1 if the job is ACTIVE", labels=["job_name"])
        input_records = GaugeMetricFamily("qtp_job_input_records",
                                          "Input record count, latest run", labels=["job_name"])
        output_records = GaugeMetricFamily("qtp_job_output_records",
                                           "Output record count, latest run", labels=["job_name"])
        error_records = GaugeMetricFamily("qtp_job_error_records",
                                          "Total error record count, latest run", labels=["job_name"])
        error_rate = GaugeMetricFamily("qtp_job_error_rate",
                                       "Error rate (0..1), latest run", labels=["job_name"])
        duration = GaugeMetricFamily("qtp_job_last_run_duration_seconds",
                                     "Duration of the latest completed run", labels=["job_name"])

        for flow in self.flows:
            jname = job_name(flow)
            job = _find_job(self.sch, jname)
            if job is None:
                up.add_metric([jname], 0)
                continue
            up.add_metric([jname], 1 if _is_active(job) else 0)

            m = _latest_run_metrics(job)
            if m is not None:
                input_records.add_metric([jname], m.input_count or 0)
                output_records.add_metric([jname], m.output_count or 0)
                error_records.add_metric([jname], m.total_error_count or 0)
                total = (m.input_count or 0)
                error_rate.add_metric([jname], (m.total_error_count or 0) / total if total else 0.0)

            secs = _latest_run_duration_seconds(job)
            if secs is not None:
                duration.add_metric([jname], secs)

        yield up
        yield input_records
        yield output_records
        yield error_records
        yield error_rate
        yield duration

        yield from self._alert_history_metrics()

    def _alert_history_metrics(self):
        """Alert/Jira counts from the WireMock request journal — real actions actually
        fired (or, once Teams/Jira are wired with real credentials, this reads from the
        same journal only while WireMock is still in the loop for local demos)."""
        from ..demo.evidence import wiremock_journal

        alerts = GaugeMetricFamily("qtp_alerts_fired_total", "Alerts posted, by severity",
                                   labels=["severity"])
        jira = GaugeMetricFamily("qtp_jira_tickets_created_total", "Jira tickets created")
        try:
            journal = wiremock_journal()
        except Exception as exc:  # noqa: BLE001 - WireMock unreachable; report zero, don't crash
            log.debug("Could not read WireMock journal: %s", exc)
            journal = {"alerts": [], "jira_tickets": []}

        by_severity = {}
        for a in journal.get("alerts", []):
            sev = str(a.get("severity", "UNKNOWN")) if isinstance(a, dict) else "UNKNOWN"
            by_severity[sev] = by_severity.get(sev, 0) + 1
        for sev, count in by_severity.items():
            alerts.add_metric([sev], count)
        jira.add_metric([], len(journal.get("jira_tickets", [])))

        yield alerts
        yield jira


def serve(port: int = DEFAULT_PORT, once: bool = False) -> int:
    from ..common.controlhub import get_control_hub

    sch = get_control_hub()
    registry = CollectorRegistry()
    registry.register(QtpCollector(sch))

    if once:
        print(generate_latest(registry).decode("utf-8"))
        return 0

    start_http_server(port, registry=registry)
    log.info("QTP metrics exporter listening on :%d/metrics (Ctrl-C to stop).", port)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        log.info("Metrics exporter stopped.")
        return 0


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="metrics", description="QTP Prometheus metrics exporter.")
    sub = parser.add_subparsers(dest="action", required=True, metavar="<action>")
    p_serve = sub.add_parser("serve", help="serve /metrics for Prometheus to scrape")
    p_serve.add_argument("--port", type=int, default=DEFAULT_PORT)
    p_serve.add_argument("--once", action="store_true", help="print one snapshot and exit")
    args = parser.parse_args(argv)
    try:
        return serve(args.port, once=args.once)
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
