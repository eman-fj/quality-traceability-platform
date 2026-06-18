# QTP — Entity Relationship Diagram

Two schemas: `quality` (Postgres — primary source/destination + audit) and `erp` (MySQL —
simulated ERP for supplier traceability).

```mermaid
erDiagram
    SUPPLIERS ||--o{ MATERIAL_BATCHES : "supplies"
    MATERIAL_BATCHES ||--o{ DEFECTIVE_PARTS : "traced to"
    MATERIAL_BATCHES ||--o{ CONFORMING_PARTS : "used in"
    DEFECTIVE_PARTS ||--o| QUARANTINE_LOG : "quarantined as"
    SUPPLIERS ||--o{ SUPPLIER_AUDIT : "rated in"

    SUPPLIERS {
        string supplier_id PK
        string name
        string country
        string accreditation_status
    }
    MATERIAL_BATCHES {
        string batch_id PK
        string supplier_id FK
        string material_grade
        string certificate_url
    }
    CONFORMING_PARTS {
        string part_id PK
        string line_id
        timestamp inspection_timestamp
        string material_batch_id FK
        string operator_id
        int schema_version
        json measurements
    }
    DEFECTIVE_PARTS {
        string part_id PK
        string line_id
        timestamp inspection_timestamp
        string material_batch_id FK
        string operator_id
        json defect_codes
        string defect_category
        string recommended_action
        string supplier_name
        string material_grade
        string certificate_url
        string quarantine_id FK
        timestamp traceability_completed_at
        int schema_version
    }
    QUARANTINE_LOG {
        string quarantine_id PK
        string part_id FK
        string status
        timestamp registered_at
        int retry_count
    }
    SUPPLIER_AUDIT {
        string supplier_id FK
        int defect_count
        float defect_correlation_score
        timestamp window_start
        timestamp window_end
    }
    SHIFT_REPORTS {
        string report_id PK
        string shift
        date report_date
        float conforming_rate
        string minio_path
        timestamp generated_at
    }
    PIPELINE_AUDIT {
        string run_id PK
        string pipeline_name
        int record_count
        int error_count
        int critical_count
        int traceability_lookups
        int quarantine_registrations
        string environment
        timestamp run_at
    }
```

## Notes
- `DEFECTIVE_PARTS.traceability_completed_at` minus `inspection_timestamp` drives the **2-hour
  SLA** check.
- `PIPELINE_AUDIT` is written by `audit_log_writer` on every pipeline run.
- `schema_version` (written by `schema_version_tagger`) appears on every part record for ISO
  audit traceability.
- `SUPPLIER_AUDIT.defect_correlation_score` backs the supplier quality risk score (stretch).
