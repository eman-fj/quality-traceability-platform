-- QTP quality warehouse schema (Postgres)
-- Loaded automatically on first container start.

CREATE SCHEMA IF NOT EXISTS quality;
SET search_path TO quality;

-- Parts that passed inspection (clean path).
CREATE TABLE conforming_parts (
    part_id              TEXT PRIMARY KEY,
    line_id              TEXT NOT NULL,
    inspection_timestamp TIMESTAMPTZ NOT NULL,
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
    inspection_timestamp     TIMESTAMPTZ NOT NULL,
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

-- Generated shift reports.
CREATE TABLE shift_reports (
    report_id        TEXT PRIMARY KEY,
    shift            TEXT,
    report_date      DATE,
    conforming_rate  NUMERIC,
    minio_path       TEXT,
    generated_at     TIMESTAMPTZ DEFAULT now()
);

-- Audit row per pipeline run (written by the audit_log_writer fragment).
CREATE TABLE pipeline_audit (
    run_id                    TEXT PRIMARY KEY,
    pipeline_name             TEXT,
    record_count              INTEGER,
    error_count               INTEGER,
    critical_count            INTEGER,
    traceability_lookups      INTEGER,
    quarantine_registrations  INTEGER,
    environment               TEXT,
    operator                  TEXT,
    run_at                    TIMESTAMPTZ DEFAULT now()
);
