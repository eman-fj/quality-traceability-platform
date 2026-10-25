# QTP: system architecture

QTP handles quality control and supplier traceability for an automotive components manufacturer
with three production lines. Every part is inspected. Under ISO 9001 and EU product liability
rules, a defective part must be traced to its raw-material supplier and batch certificate within
two hours. The diagram shows the system as built.

For slides, use the one-page overview in [`architecture.svg`](architecture.svg).

> Every pipeline and fragment is built and published to StreamSets Control Hub through the
> Python SDK (`qtp` CLI); none is drawn by hand on the canvas. Credentials reach the pipelines
> as Control Hub job runtime parameters when the job is created, so they never appear in the
> pipeline definition or the committed JSON exports.

```mermaid
%%{init: {"theme": "base", "themeVariables": {"fontFamily": "Segoe UI, Helvetica, Arial, sans-serif", "fontSize": "14px", "lineColor": "#64748b", "clusterBkg": "#f8fafc", "clusterBorder": "#cbd5e1"}}}%%
flowchart TB
    classDef src fill:#1e293b,stroke:#475569,color:#f1f5f9
    classDef sdc fill:#e0ecff,stroke:#2563eb,color:#0b2a5b
    classDef gate fill:#fff7ed,stroke:#ea580c,color:#7c2d12
    classDef store fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef ext fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    classDef obs fill:#fef3c7,stroke:#d97706,color:#78350f

    subgraph SRC["Event backbone"]
        SIM["inspection_simulator<br/>3 lines · ~3 events/s · periodic CRITICAL"]:::src
        K1[["Kafka · quality.inspections"]]:::src
        SIM --> K1
    end

    subgraph P1["Pipeline A: manufacturing__quality_ingest"]
        direction TB
        A_ENR["kafka_header_enricher →<br/>data_quality_tagger →<br/>schema_version_tagger"]:::sdc
        A_SEL{"Stream Selector<br/>defect_codes present?"}:::gate
        A_FR["Field Remover<br/>(drop non-column fields)"]:::sdc
        K1 --> A_ENR --> A_SEL
        A_SEL -->|clean| A_FR
        A_SEL -->|defect| K2[["Kafka · quality.defects.major"]]:::src
    end

    subgraph P2["Pipeline B: manufacturing__defect_enrichment"]
        direction TB
        B_ENR["3 inline enrichers →<br/>JDBC Lookup (ERP join) →<br/>HTTP classify →<br/>defect_codes flatten (JS)"]:::sdc
        B_Q["HTTP quarantine<br/>(3 retries, exponential backoff)"]:::sdc
        B_ASEL{"Stream Selector<br/>severity"}:::gate
        B_GATE["Rollup gate (stateful JS)<br/>1 alert per line per 5 min<br/>+ suppressed count"]:::gate
        K2 --> B_ENR --> B_Q --> B_FR2["Field Remover → JDBC Producer"]:::sdc
        B_ENR --> B_ASEL
        B_ASEL -->|CRITICAL / MAJOR| B_GATE
    end

    subgraph EXT["External systems (real and simulated)"]
        ERP[("MySQL ERP<br/>suppliers · material_batches")]:::ext
        WM["WireMock<br/>/defects/classify · /quarantine/register<br/>+ alert audit journal"]:::ext
        TEAMS["Microsoft Teams<br/>Adaptive Card (Workflows webhook)"]:::ext
        JIRA["Jira Cloud<br/>REST v3 create issue (ADF)"]:::ext
        MAIL["Mailhog SMTP<br/>MINOR email"]:::ext
        B_ENR -.lookup.-> ERP
        B_ENR -.classify + quarantine.-> WM
        B_GATE -->|CRITICAL| JIRA
        B_GATE -->|CRITICAL / MAJOR| TEAMS
        B_ASEL -->|MINOR| MAIL
        B_GATE -.audit every alert.-> WM
    end

    subgraph STORE["Warehouse and object store"]
        PG[("Postgres · quality<br/>conforming_parts · defective_parts<br/>quarantine_log · supplier_audit<br/>shift_reports · pipeline_audit · dlq_recovery")]:::store
        MINIO[("MinIO (TLS)<br/>conforming Parquet · reports · error DLQ")]:::store
        A_FR --> PG
        A_FR --> MINIO
        B_FR2 --> PG
        A_ENR -. errors .-> MINIO
        B_ENR -. errors .-> MINIO
    end

    subgraph OBS["SDK control plane and observability"]
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

- **Clean path** (high volume). Records are enriched, tagged, and stamped with a schema version,
  pass through a Field Remover, and land in `conforming_parts` in Postgres.
  `manufacturing__conforming_archive` also writes them to MinIO as Parquet.
- **Defect path.** Defects are republished to `quality.defects.major` and enriched: an ERP join
  from batch to supplier, an HTTP classification call, a JavaScript step that flattens the
  `defect_codes` list into a string, and a mandatory quarantine call with up to three retries
  and exponential backoff. The record then lands in `defective_parts` with its `quarantine_id`.
- **Severity routing.** A Stream Selector splits CRITICAL, MAJOR, and MINOR. CRITICAL goes to
  Teams and Jira, MAJOR to Teams, and MINOR to email. Every CRITICAL and MAJOR alert is also
  written to the WireMock audit journal.
- **Rollup gate.** A stateful JavaScript stage sends at most one Teams or Jira call per line in
  each five-minute window and adds the number it held back to the next alert. This keeps the
  Teams connector and Jira from being flooded, and no audit data is dropped.
- **Two-hour SLA loop.** `qtp sla scan` polls `defective_parts` for parts past the window
  without completed traceability and escalates each one with a CRITICAL alert and a Jira ticket.
- **Governance.** Postgres, MySQL, MinIO, and Kafka are reached through named Control Hub
  connections. Fragments are pinned to immutable commit tags, job secrets are injected at
  runtime, and every external call retries with a MinIO dead-letter queue behind it.
