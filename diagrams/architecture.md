# QTP — Six-Layer Architecture

End-to-end data flow for Production Line Quality Control & Supplier Traceability.
A rendered image lives at [`architecture.svg`](architecture.svg) (also copied to `/evidence`).

```mermaid
flowchart TB
    subgraph L1["1 · Factory Event Backbone"]
        SIM["inspection_simulator.py<br/>3 lines · ~1 evt/s/line"]
        K[("Kafka<br/>quality.inspections<br/>quality.defects.critical<br/>quality.defects.major")]
        SIM --> K
    end

    subgraph L2["2 · StreamSets Quality Intelligence Layer"]
        ING["manufacturing__quality_ingest<br/>header enrich · quality tag · schema stamp"]
        ROUTE{"defect_codes<br/>empty?"}
        ENR["manufacturing__defect_enrichment<br/>ERP join · classify · quarantine"]
        ARC["manufacturing__conforming_archive"]
        DLQ["dead_letter_queue_handler"]
        K --> ING --> ROUTE
        ROUTE -->|clean| ARC
        ROUTE -->|defect| ENR
        ING -. failures .-> DLQ
        ENR -. failures .-> DLQ
    end

    subgraph L3["3 · Traceability / Supplier Intelligence"]
        ERP[("MySQL ERP<br/>material_batches · suppliers")]
        CLS["WireMock POST /defects/classify<br/>CRITICAL/MAJOR/MINOR · SCRAP/REWORK/HOLD"]
        QAR["WireMock PUT /quarantine/register<br/>retry x3 backoff → quarantine_id"]
        ENR --> ERP
        ENR --> CLS
        ENR --> QAR
    end

    subgraph L4["4 · Compliance Control Tower"]
        PG[("Postgres · quality schema<br/>conforming_parts · defective_parts<br/>quarantine_log · supplier_audit<br/>shift_reports · pipeline_audit")]
        SLA["qtp sla check<br/>2-hour SLA · breach-risk"]
        ALERT["alert_notification_dispatcher<br/>INFO→mail · WARN→Teams · CRITICAL→Teams+Jira"]
        RATE["manufacturing__defect_rate_monitor<br/>>2% per line → CRITICAL"]
        ARC --> PG
        ENR --> PG
        PG --> SLA
        SLA --> ALERT
        RATE --> ALERT
    end

    subgraph L5["5 · Reporting & Executive Observability"]
        GRAF["Grafana command centre<br/>defect rate/line · CRITICAL today<br/>top-3 codes · worst supplier"]
        RPT["qtp report shift → Excel<br/>MinIO + Mailhog email"]
        MINIO[("MinIO<br/>conforming · defects · reports · dlq")]
        PG --> GRAF
        PG --> RPT
        ARC --> MINIO
        DLQ --> MINIO
        RPT --> MINIO
    end

    subgraph L6["6 · Demo & Evidence Layer"]
        DEMO["qtp demo run"]
        PACK["qtp evidence pack --part<br/>auditor-ready JSON/PDF"]
        DEMO -.drives.-> SIM
        PG --> PACK
    end
```

## Reading the diagram
- **Clean path** (Layer 2 → Archive → Postgres + MinIO Parquet) is the high-volume happy path.
- **Defect path** fans out to ERP traceability, classification, and quarantine, then lands
  enriched in Postgres with a `quarantine_id`.
- **The 2-hour SLA loop** (Layer 4) polls defective parts lacking completed traceability and
  escalates before the window closes.
- **Every external call** (ERP, classify, quarantine) is wrapped with retry + DLQ.
