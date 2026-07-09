# QTP: System Architecture

Production-line quality control and supplier traceability for an automotive-components
manufacturer (3 lines). Every part is inspected; defective parts must be traced to their
raw-material supplier and batch certificate **within 2 hours** (ISO 9001 / EU product
liability). This diagram reflects the *as-built* system, not an aspiration.

> Authoring model: every pipeline and fragment is built and published to StreamSets Control
> Hub through the Python SDK (`qtp` CLI) - nothing is hand-drawn on the canvas. Credentials
> are injected as Control Hub **job runtime parameters** at job-creation time, never baked
> into the pipeline definition or the committed JSON exports.

```mermaid
flowchart TB
    classDef src fill:#1f2937,stroke:#4b5563,color:#e5e7eb
    classDef sdc fill:#0e3a5f,stroke:#2563eb,color:#e6f0ff
    classDef store fill:#14532d,stroke:#16a34a,color:#dcfce7
    classDef ext fill:#4c1d95,stroke:#7c3aed,color:#ede9fe
    classDef obs fill:#7c2d12,stroke:#ea580c,color:#ffedd5

    subgraph SRC["Event backbone"]
        SIM["inspection_simulator<br/>3 lines · ~3 evt/s · periodic CRITICAL"]:::src
        K1[["Kafka · quality.inspections"]]:::src
        SIM --> K1
    end

    subgraph P1["Pipeline A - manufacturing__quality_ingest"]
        direction TB
        A_ENR["kafka_header_enricher →<br/>data_quality_tagger →<br/>schema_version_tagger"]:::sdc
        A_SEL{"Stream Selector<br/>defect_codes present?"}:::sdc
        A_FR["Field Remover<br/>(drop non-column fields)"]:::sdc
        K1 --> A_ENR --> A_SEL
        A_SEL -->|clean| A_FR
        A_SEL -->|defect| K2[["Kafka · quality.defects.major"]]:::src
    end

    subgraph P2["Pipeline B - manufacturing__defect_enrichment"]
        direction TB
        B_ENR["3 inline enrichers →<br/>JDBC Lookup (ERP join) →<br/>HTTP classify →<br/>defect_codes flatten (JS)"]:::sdc
        B_Q["HTTP quarantine<br/>(retry ×3 exp-backoff)"]:::sdc
        B_ASEL{"Stream Selector<br/>severity"}:::sdc
        B_GATE["Rollup gate (stateful JS)<br/>1 alert / line / 5 min<br/>+ suppressed count"]:::sdc
        K2 --> B_ENR --> B_Q --> B_FR2["Field Remover → JDBC Producer"]:::sdc
        B_ENR --> B_ASEL
        B_ASEL -->|CRITICAL / MAJOR| B_GATE
    end

    subgraph EXT["External systems (real + simulated)"]
        ERP[("MySQL ERP<br/>suppliers · material_batches")]:::ext
        WM["WireMock<br/>/defects/classify · /quarantine/register<br/>+ alert audit journal"]:::ext
        TEAMS["Microsoft Teams<br/>Adaptive Card (Workflows webhook)"]:::ext
        JIRA["Jira Cloud<br/>REST v3 create-issue (ADF)"]:::ext
        MAIL["Mailhog SMTP<br/>MINOR email"]:::ext
        B_ENR -.lookup.-> ERP
        B_ENR -.classify + quarantine.-> WM
        B_GATE -->|CRITICAL| JIRA
        B_GATE -->|CRITICAL / MAJOR| TEAMS
        B_ASEL -->|MINOR| MAIL
        B_GATE -.audit every alert.-> WM
    end

    subgraph STORE["Warehouse & object store"]
        PG[("Postgres · quality<br/>conforming_parts · defective_parts<br/>quarantine_log · supplier_audit<br/>shift_reports · pipeline_audit · dlq_recovery")]:::store
        MINIO[("MinIO (TLS)<br/>conforming Parquet · reports · error DLQ")]:::store
        A_FR --> PG
        A_FR --> MINIO
        B_FR2 --> PG
        A_ENR -. errors .-> MINIO
        B_ENR -. errors .-> MINIO
    end

    subgraph OBS["SDK control plane & observability"]
        SLA["qtp sla scan<br/>2-hour traceability SLA"]:::obs
        MON["qtp monitor defect-rate<br/>>2% per line"]:::obs
        RPT["qtp report shift → Excel<br/>MinIO + Mailhog"]:::obs
        METRICS["qtp metrics serve<br/>Prometheus exporter"]:::obs
        GRAF["Grafana<br/>quality + pipeline-health dashboards"]:::obs
        PG --> SLA --> JIRA
        PG --> MON
        PG --> RPT --> MINIO
        METRICS --> GRAF
        PG --> GRAF
    end
```

## Reading the diagram

- **Clean path** (high volume): ingest → enrich/tag/stamp → Field Remover → `conforming_parts`
  in Postgres, and conforming Parquet in MinIO via `manufacturing__conforming_archive`.
- **Defect path**: republished to `quality.defects.major`, then enriched - ERP batch→supplier
  join, HTTP classification, a JS step that flattens the `defect_codes` list to a clean
  string, and a mandatory quarantine call (3× exponential-backoff retry) - before landing in
  `defective_parts` with a `quarantine_id`.
- **Severity fan-out**: a Stream Selector splits CRITICAL / MAJOR / MINOR. CRITICAL → Teams +
  Jira; MAJOR → Teams; MINOR → email. Every CRITICAL/MAJOR also hits the WireMock audit sink.
- **Rollup gate**: a stateful JavaScript stage caps outbound Teams/Jira calls to one per line
  per 5-minute window and carries the suppressed count onto the next alert - this is what
  keeps the Teams connector and Jira from being flooded, without dropping any audit data.
- **2-hour SLA loop**: `qtp sla scan` polls `defective_parts` for parts past the window with
  no completed traceability and escalates (CRITICAL alert + Jira ticket).
- **Governance**: named Control Hub connections for Postgres/MySQL/MinIO/Kafka; fragments
  pinned to immutable commit tags; job secrets injected at runtime; every external call is
  retry-wrapped with a MinIO dead-letter queue behind it.
