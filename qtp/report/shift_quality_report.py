"""shift_quality_report.py - end-of-shift quality report for production management.

Generates the shift quality report the ISO 9001 quality manager receives at the end of
every shift. For a given ``--shift`` and ``--date`` it:

  * queries Postgres for: total parts inspected, conforming rate, defect breakdown by
    category, parts currently in quarantine, and the suppliers most correlated with
    defects, all per production line;
  * joins the MySQL ERP to attach each top supplier's accreditation status and country;
  * builds an Excel workbook (summary sheet + per-line sheet + a defects-by-line chart);
  * uploads it to the MinIO shift-reports bucket and emails it to the quality manager
    via Mailhog; and
  * records the run in the quality.shift_reports table.

Host-side connection details come from .env (nothing hardcoded). The MinIO upload, the
email, and the MySQL enrichment each degrade gracefully - a missing sink never stops the
report from being generated.
"""

import os
import smtplib
import tempfile
from datetime import date as date_cls
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import psycopg2
import psycopg2.extras

from ..common.env import mailhog_smtp, minio_settings, mysql_conn_params, pg_conn_params
from ..common.util import get_logger

log = get_logger()

CONFORMING_BUCKET = "kermit-shift-reports"
DEFAULT_RECIPIENT = "quality.manager@kermit.local"
_DATE_FILTER = "inspection_timestamp::timestamptz::date = %s::date"


# --------------------------------------------------------------------------- data
def collect_metrics(conn, shift: str, day: str) -> dict:
    """Gather all shift KPIs from the quality warehouse for one shift/date."""
    args = (shift, day)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT count(*) AS n FROM quality.conforming_parts WHERE shift=%s AND {_DATE_FILTER}",
            args)
        conforming = cur.fetchone()["n"]

        cur.execute(
            f"SELECT count(*) AS n FROM quality.defective_parts WHERE shift=%s AND {_DATE_FILTER}",
            args)
        defective = cur.fetchone()["n"]

        cur.execute(
            f"""SELECT defect_category, count(*) AS n FROM quality.defective_parts
                WHERE shift=%s AND {_DATE_FILTER} GROUP BY defect_category ORDER BY n DESC""",
            args)
        by_category = [(r["defect_category"], r["n"]) for r in cur.fetchall()]

        cur.execute(
            f"""SELECT count(*) AS n FROM quality.defective_parts
                WHERE shift=%s AND {_DATE_FILTER} AND quarantine_id IS NOT NULL""",
            args)
        in_quarantine = cur.fetchone()["n"]

        # Per-line conforming + defective in one pass.
        cur.execute(
            f"""SELECT line_id,
                       sum(CASE WHEN src='C' THEN 1 ELSE 0 END) AS conforming,
                       sum(CASE WHEN src='D' THEN 1 ELSE 0 END) AS defective
                FROM (
                    SELECT line_id, 'C' AS src FROM quality.conforming_parts
                        WHERE shift=%s AND {_DATE_FILTER}
                    UNION ALL
                    SELECT line_id, 'D' AS src FROM quality.defective_parts
                        WHERE shift=%s AND {_DATE_FILTER}
                ) u GROUP BY line_id ORDER BY line_id""",
            (shift, day, shift, day))
        per_line = [(r["line_id"], int(r["conforming"]), int(r["defective"])) for r in cur.fetchall()]

        cur.execute(
            f"""SELECT supplier_id, supplier_name, count(*) AS defects
                FROM quality.defective_parts WHERE shift=%s AND {_DATE_FILTER}
                GROUP BY supplier_id, supplier_name ORDER BY defects DESC LIMIT 3""",
            args)
        top_suppliers = [dict(r) for r in cur.fetchall()]

    total = conforming + defective
    return {
        "total_inspected": total,
        "conforming": conforming,
        "defective": defective,
        "conforming_rate": round(100.0 * conforming / total, 2) if total else 0.0,
        "in_quarantine": in_quarantine,
        "by_category": by_category,
        "per_line": per_line,
        "top_suppliers": top_suppliers,
    }


def enrich_supplier_accreditation(supplier_ids: list[str]) -> dict:
    """Attach country + accreditation_status from the MySQL ERP. Degrades to an empty
    map if pymysql is unavailable or the ERP can't be reached - the report still builds."""
    if not supplier_ids:
        return {}
    try:
        import pymysql  # lazy: optional enrichment
    except ImportError:
        log.warning("pymysql not installed; skipping supplier accreditation enrichment.")
        return {}
    try:
        conn = pymysql.connect(**mysql_conn_params())
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not reach ERP (MySQL) for accreditation: %s", exc)
        return {}
    try:
        placeholders = ",".join(["%s"] * len(supplier_ids))
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT supplier_id, name, country, accreditation_status FROM suppliers "
                f"WHERE supplier_id IN ({placeholders})", supplier_ids)
            # Canonical UTF-8 name from the ERP (defective_parts.supplier_name is mojibake -
            # the pipeline's JDBC lookup connection wasn't UTF-8).
            return {row[0]: {"name": row[1], "country": row[2], "accreditation_status": row[3]}
                    for row in cur.fetchall()}
    finally:
        conn.close()


# ------------------------------------------------------------------------- excel
def _fix_mojibake(s: str | None) -> str | None:
    """Repair UTF-8-stored-as-Latin-1 double-encoding (e.g. 'PrÃ©cision' -> 'Précision').

    Idempotent on clean input: ASCII round-trips unchanged, and correctly-encoded
    accented text raises on the latin1 step and is returned as-is.
    """
    if not s:
        return s
    try:
        # cp1252 (not latin1): the corruption maps bytes like 0x9C -> 'œ', a Windows-1252
        # code point, so 'OÃœ' (Ü) only round-trips through cp1252.
        return s.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def build_workbook(metrics: dict, shift: str, day: str, accreditation: dict):
    from openpyxl import Workbook
    from openpyxl.chart import BarChart, Reference
    from openpyxl.styles import Font

    bold = Font(bold=True)
    wb = Workbook()

    # --- Summary sheet --------------------------------------------------------
    ws = wb.active
    ws.title = "Summary"
    ws["A1"] = "Shift Quality Report"
    ws["A1"].font = Font(bold=True, size=14)
    ws["A2"] = "Shift"; ws["B2"] = shift
    ws["A3"] = "Date"; ws["B3"] = day
    ws["A4"] = "Generated"; ws["B4"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    row = 6
    ws.cell(row, 1, "Metric").font = bold
    ws.cell(row, 2, "Value").font = bold
    for label, value in [
        ("Total parts inspected", metrics["total_inspected"]),
        ("Conforming", metrics["conforming"]),
        ("Defective", metrics["defective"]),
        ("Conforming rate (%)", metrics["conforming_rate"]),
        ("Parts in quarantine", metrics["in_quarantine"]),
    ]:
        row += 1
        ws.cell(row, 1, label)
        ws.cell(row, 2, value)

    row += 2
    ws.cell(row, 1, "Defect category").font = bold
    ws.cell(row, 2, "Count").font = bold
    for cat, n in metrics["by_category"]:
        row += 1
        ws.cell(row, 1, cat)
        ws.cell(row, 2, n)

    row += 2
    for i, h in enumerate(["Top defect-correlated supplier", "Country", "Accreditation", "Defects"], start=1):
        ws.cell(row, i, h).font = bold
    for sup in metrics["top_suppliers"]:
        row += 1
        acc = accreditation.get(sup["supplier_id"], {})
        ws.cell(row, 1, _fix_mojibake(acc.get("name") or sup["supplier_name"]))
        ws.cell(row, 2, acc.get("country", "-"))
        ws.cell(row, 3, acc.get("accreditation_status", "-"))
        ws.cell(row, 4, sup["defects"])
    ws.column_dimensions["A"].width = 32
    ws.column_dimensions["C"].width = 16

    # --- Per-line sheet + chart ----------------------------------------------
    wl = wb.create_sheet("Per-Line")
    for i, h in enumerate(["Line", "Conforming", "Defective", "Total", "Defect rate (%)"], start=1):
        wl.cell(1, i, h).font = bold
    r = 1
    for line_id, conforming, defective in metrics["per_line"]:
        r += 1
        total = conforming + defective
        wl.cell(r, 1, line_id)
        wl.cell(r, 2, conforming)
        wl.cell(r, 3, defective)
        wl.cell(r, 4, total)
        wl.cell(r, 5, round(100.0 * defective / total, 2) if total else 0.0)

    if r > 1:
        chart = BarChart()
        chart.title = "Defective parts by line"
        chart.y_axis.title = "Defective"
        chart.x_axis.title = "Line"
        data = Reference(wl, min_col=3, min_row=1, max_row=r)        # Defective column + header
        cats = Reference(wl, min_col=1, min_row=2, max_row=r)        # Line ids
        chart.add_data(data, titles_from_data=True)
        chart.set_categories(cats)
        wl.add_chart(chart, "G2")
    wl.column_dimensions["A"].width = 12
    return wb


# ----------------------------------------------------------------- sinks (graceful)
def _cert_path() -> str | bool:
    # certs/public.crt lives at the repo root; localhost is a SAN so verification works.
    cert = Path(__file__).resolve().parents[2] / "certs" / "public.crt"
    return str(cert) if cert.exists() else False


def upload_to_minio(local_path: str, key: str) -> str | None:
    try:
        import boto3
        s = minio_settings()
        verify = _cert_path()
        if verify is False:
            import urllib3
            urllib3.disable_warnings()
        client = boto3.client(
            "s3", endpoint_url=s["endpoint_url"],
            aws_access_key_id=s["access_key"], aws_secret_access_key=s["secret_key"],
            verify=verify)
        client.upload_file(local_path, CONFORMING_BUCKET, key)
        path = f"s3://{CONFORMING_BUCKET}/{key}"
        log.info("Uploaded report to %s", path)
        return path
    except Exception as exc:  # noqa: BLE001
        log.warning("MinIO upload failed (%s); report generated locally only.", exc)
        return None


def email_report(local_path: str, shift: str, day: str, recipient: str) -> bool:
    try:
        msg = EmailMessage()
        msg["Subject"] = f"Shift Quality Report - shift {shift} {day}"
        msg["From"] = "qtp-reports@kermit.local"
        msg["To"] = recipient
        msg.set_content(
            f"Attached is the quality report for shift {shift} on {day}.\n"
            "Generated automatically by the Quality & Traceability Platform.")
        with open(local_path, "rb") as f:
            msg.add_attachment(
                f.read(), maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=os.path.basename(local_path))
        host, port = mailhog_smtp()
        with smtplib.SMTP(host, port, timeout=10) as smtp:
            smtp.send_message(msg)
        log.info("Emailed report to %s via Mailhog (%s:%s)", recipient, host, port)
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("Emailing report failed (%s); skipped.", exc)
        return False


def record_shift_report(conn, shift: str, day: str, conforming_rate: float, minio_path: str | None):
    report_id = f"{day}_{shift}"
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO quality.shift_reports (report_id, shift, report_date, conforming_rate, minio_path)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (report_id) DO UPDATE
                 SET conforming_rate = EXCLUDED.conforming_rate,
                     minio_path = EXCLUDED.minio_path,
                     generated_at = now()""",
            (report_id, shift, day, conforming_rate, minio_path))
    conn.commit()


# ---------------------------------------------------------------------- orchestrate
def generate(shift: str, day: str, recipient: str = DEFAULT_RECIPIENT) -> int:
    conn = psycopg2.connect(**pg_conn_params())
    try:
        metrics = collect_metrics(conn, shift, day)
        if metrics["total_inspected"] == 0:
            log.warning("No parts inspected for shift %s on %s - nothing to report.", shift, day)
            return 0

        accreditation = enrich_supplier_accreditation(
            [s["supplier_id"] for s in metrics["top_suppliers"] if s["supplier_id"]])
        wb = build_workbook(metrics, shift, day, accreditation)

        fname = f"quality_report_{shift}_{day}.xlsx"
        local_path = os.path.join(tempfile.gettempdir(), fname)
        wb.save(local_path)
        log.info("Built %s (conforming rate %.2f%%, %d inspected, %d defective).",
                 fname, metrics["conforming_rate"], metrics["total_inspected"], metrics["defective"])

        key = f"shift-reports/{day}/{shift}/{fname}"
        minio_path = upload_to_minio(local_path, key)
        email_report(local_path, shift, day, recipient)
        record_shift_report(conn, shift, day, metrics["conforming_rate"], minio_path)
    finally:
        conn.close()

    print(f"Shift report ready: shift {shift} {day} | conforming {metrics['conforming_rate']}% | "
          f"{metrics['defective']} defective | minio={minio_path or 'n/a'}")
    return 0


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="shift_quality_report",
        description="End-of-shift quality report (Excel → MinIO + Mailhog).")
    parser.add_argument("--shift", required=True, choices=["A", "B", "C"], help="production shift")
    parser.add_argument("--date", default=date_cls.today().isoformat(),
                        help="report date YYYY-MM-DD (default: today)")
    parser.add_argument("--email", default=DEFAULT_RECIPIENT, help="quality manager email")
    args = parser.parse_args(argv)
    try:
        return generate(args.shift, args.date, args.email)
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
