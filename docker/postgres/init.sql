-- QTP quality warehouse schema (Postgres)
-- Loaded automatically on first container start.

CREATE SCHEMA IF NOT EXISTS quality;
SET search_path TO quality;

-- Parts that passed inspection (clean path).
-- inspection_timestamp is stored as TEXT (canonical ISO-8601 from the inspection
-- station, e.g. 2026-06-19T07:24:54.676771+00:00). The StreamSets JDBC Producer
-- coerces strings to TIMESTAMP client-side via Timestamp.valueOf, which rejects the
-- ISO 'T'/offset/microsecond form; storing the canonical string and casting in queries
-- (inspection_timestamp::timestamptz) is lossless and avoids that coercion.
CREATE TABLE conforming_parts (
    part_id              TEXT PRIMARY KEY,
    line_id              TEXT NOT NULL,
    inspection_timestamp TEXT NOT NULL,
    material_batch_id    TEXT,
    operator_id          TEXT,
    shift                TEXT,
    measurements         JSONB,
    schema_version       INTEGER,
    ingested_at          TIMESTAMPTZ DEFAULT now()
);

-- Parts that failed inspection (defect path), enriched with full traceability.
CREATE TABLE defective_parts (
    part_id                  TEXT PRIMARY KEY,
    line_id                  TEXT NOT NULL,
    inspection_timestamp     TEXT NOT NULL,  -- ISO-8601 string; cast to timestamptz in SLA queries
    material_batch_id        TEXT,
    operator_id              TEXT,
    shift                    TEXT,
    defect_codes             JSONB,
    defect_category          TEXT,          -- CRITICAL | MAJOR | MINOR
    recommended_action       TEXT,          -- SCRAP | REWORK | HOLD
    -- traceability (joined from MySQL ERP)
    supplier_id              TEXT,
    supplier_name            TEXT,
    material_grade           TEXT,
    certificate_url          TEXT,
    -- quarantine + SLA
    quarantine_id            TEXT,
    traceability_completed_at TIMESTAMPTZ,
    schema_version           INTEGER,
    ingested_at              TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_defective_line   ON defective_parts (line_id);
CREATE INDEX idx_defective_cat    ON defective_parts (defect_category);
CREATE INDEX idx_defective_trace  ON defective_parts (traceability_completed_at);

-- Quarantine register (one row per quarantined part).
CREATE TABLE quarantine_log (
    quarantine_id  TEXT PRIMARY KEY,
    part_id        TEXT NOT NULL,
    status         TEXT,
    registered_at  TIMESTAMPTZ DEFAULT now(),
    retry_count    INTEGER DEFAULT 0
);

-- Supplier defect-correlation snapshots (backs the supplier risk score).
CREATE TABLE supplier_audit (
    supplier_id              TEXT,
    defect_count             INTEGER,
    defect_correlation_score NUMERIC,
    window_start             TIMESTAMPTZ,
    window_end               TIMESTAMPTZ
);

-- Recovered dead-letter records (written by the dlq_recovery pipeline, which reads the
-- MinIO error bucket and enriches each record with the dead_letter_queue_handler fragment).
-- SDC discards per-stage error metadata when error records are re-read from the archive,
-- so recovery captures the failed record's identity (part_id/line_id) + source pipeline +
-- time — enough to locate, investigate, and reprocess the part.
CREATE TABLE dlq_recovery (
    id                  BIGSERIAL PRIMARY KEY,
    dlq_source_pipeline TEXT,
    part_id             TEXT,
    line_id             TEXT,
    dlq_error_timestamp TIMESTAMPTZ,
    recovered_at        TIMESTAMPTZ DEFAULT now()
);

-- Generated shift reports.
CREATE TABLE shift_reports (
    report_id        TEXT PRIMARY KEY,
    shift            TEXT,
    report_date      DATE,
    conforming_rate  NUMERIC,
    minio_path       TEXT,
    generated_at     TIMESTAMPTZ DEFAULT now()
);

-- Audit row per pipeline run, written by the shared audit_log_writer fragment
-- (Expression Evaluator -> JDBC Producer). Columns match the fragment's output fields so
-- its auto field->column mapping works. The per-run metrics (record/error/critical/
-- traceability/quarantine counts, report generation time) are carried in audit_metrics as
-- JSON — the fragment's AUDIT_METRICS_JSON parameter, set at run time.
CREATE TABLE pipeline_audit (
    id            BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT,
    run_ts        TIMESTAMPTZ,
    operator      TEXT,
    environment   TEXT,
    audit_metrics TEXT,
    recorded_at   TIMESTAMPTZ DEFAULT now()
);
