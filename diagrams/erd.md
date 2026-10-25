# QTP: entity relationship diagram

The traceability chain spans two databases. `erp` (MySQL) is the simulated supplier and ERP
system of record. `quality` (Postgres) is the quality warehouse, holding pipeline output, SLA
state, and audit records. The link between them is `defective_parts.material_batch_id →
material_batches.batch_id`, which the `defect_enrichment` JDBC Lookup resolves at runtime.

> The column types below are the deployed schema. `inspection_timestamp` is stored as TEXT
> because the StreamSets JDBC Producer did not reliably convert the ISO-8601 string into a
> `TIMESTAMP` column. The value is kept as received and cast in queries.

```mermaid
%%{init: {"theme": "base", "themeVariables": {"fontFamily": "Segoe UI, Helvetica, Arial, sans-serif", "primaryColor": "#e0ecff", "primaryBorderColor": "#2563eb", "primaryTextColor": "#0b2a5b", "lineColor": "#64748b", "attributeBackgroundColorOdd": "#ffffff", "attributeBackgroundColorEven": "#f1f5f9"}}}%%
erDiagram
    SUPPLIERS ||--o{ MATERIAL_BATCHES : supplies
    MATERIAL_BATCHES ||--o{ DEFECTIVE_PARTS : "traced to"
    MATERIAL_BATCHES ||--o{ CONFORMING_PARTS : "used in"
    DEFECTIVE_PARTS ||--o| QUARANTINE_LOG : "quarantined as"
    SUPPLIERS ||--o{ SUPPLIER_AUDIT : "risk-scored in"

    SUPPLIERS {
        varchar supplier_id PK
        varchar name
        varchar country
        varchar accreditation_status
    }
    MATERIAL_BATCHES {
        varchar batch_id PK
        varchar supplier_id FK
        varchar material_grade
        varchar certificate_url
    }
    CONFORMING_PARTS {
        text part_id
        text line_id
        text inspection_timestamp
        text material_batch_id FK
        text operator_id
        text shift
        jsonb measurements
        integer schema_version
        timestamptz ingested_at
    }
    DEFECTIVE_PARTS {
        text part_id
        text line_id
        text inspection_timestamp
        text material_batch_id FK
        text operator_id
        text shift
        jsonb defect_codes
        text defect_category
        text recommended_action
        text supplier_id
        text supplier_name
        text material_grade
        text certificate_url
        text quarantine_id FK
        timestamptz traceability_completed_at
        integer schema_version
        timestamptz ingested_at
    }
    QUARANTINE_LOG {
        text quarantine_id PK
        text part_id FK
        text status
        timestamptz registered_at
        integer retry_count
    }
    SUPPLIER_AUDIT {
        text supplier_id FK
        integer defect_count
        numeric defect_correlation_score
        timestamptz window_start
        timestamptz window_end
    }
    SHIFT_REPORTS {
        text report_id PK
        text shift
        date report_date
        numeric conforming_rate
        text minio_path
        timestamptz generated_at
    }
    PIPELINE_AUDIT {
        bigint id PK
        text pipeline_name
        timestamptz run_ts
        text operator
        text environment
        text audit_metrics
        timestamptz recorded_at
    }
    DLQ_RECOVERY {
        bigint id PK
        text dlq_source_pipeline
        text part_id
        text line_id
        timestamptz dlq_error_timestamp
        timestamptz recovered_at
    }
```

## Notes

- The **two-hour SLA** is `traceability_completed_at - inspection_timestamp` on
  `DEFECTIVE_PARTS`. `qtp sla scan` escalates any part past the window whose
  `traceability_completed_at` is still null.
- **`PIPELINE_AUDIT`** is written by the `audit_log_writer` fragment, one row per pipeline run.
  `audit_metrics` holds the serialized record, error, and critical counts for that run.
- **`DLQ_RECOVERY`** records which records failed a stage and were archived to the MinIO
  dead-letter queue. The Data Collector error-record format drops per-stage metadata when it is
  read back, so recovery rebuilds `part_id`, `line_id`, the source, and the time rather than the
  full failed record.
- **`schema_version`**, set by `schema_version_tagger`, is on every part row. This is the ISO
  control that requires a schema version on every record.
- **`SUPPLIER_AUDIT.defect_correlation_score`** drives the "worst supplier" Grafana panel and the
  supplier risk section of the shift report.
- No database foreign key crosses the MySQL and Postgres boundary. The pipeline enforces the
  relationship through the JDBC Lookup, and the ERD shows it as a logical link.
