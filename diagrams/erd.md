# QTP: Entity Relationship Diagram

Two databases span the traceability chain: **`erp`** (MySQL - simulated supplier/ERP system
of record) and **`quality`** (Postgres - the quality warehouse: pipeline destinations, SLA
state, audit). The join across the boundary is `defective_parts.material_batch_id →
material_batches.batch_id`, resolved at runtime by the `defect_enrichment` JDBC Lookup.

> Column types below are the *actual* deployed schema. Note `inspection_timestamp` is stored
> as **TEXT** (StreamSets' JDBC Producer would not coerce the ISO-8601 string into a
> `TIMESTAMP` column reliably, so the value is kept verbatim and cast in queries) - a real
> engineering decision, documented rather than hidden.

```mermaid
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

- **2-hour SLA** is computed as `traceability_completed_at - inspection_timestamp` on
  `DEFECTIVE_PARTS`; `qtp sla scan` escalates any part past the window with the field null.
- **`PIPELINE_AUDIT`** is written by the `audit_log_writer` fragment; `audit_metrics` holds a
  serialized metrics blob (record/error/critical counts) per run - one row per pipeline run.
- **`DLQ_RECOVERY`** captures the identity of records that failed a stage and were archived to
  the MinIO DLQ (SDC's error-record format discards per-stage metadata on re-read, so recovery
  reconstructs `part_id`/`line_id`/source/time rather than the full failed record).
- **`schema_version`** (from `schema_version_tagger`) is present on every part row - the ISO
  "schema version on every record" control.
- **`SUPPLIER_AUDIT.defect_correlation_score`** backs the "worst supplier" Grafana panel and
  the shift report's supplier-risk section.
- Referential integrity across the MySQL↔Postgres boundary is enforced *in the pipeline*
  (the JDBC Lookup), not by a database FK - the ERD shows the logical relationship.
