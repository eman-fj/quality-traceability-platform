# QTP: sequence diagram for one CRITICAL defect

This diagram follows a single CRITICAL part from inspection to a closed two-hour traceability
record, across every system it touches. The demo and the evidence pack reproduce the same path
with fixed data.

```mermaid
%%{init: {"theme": "base", "themeVariables": {"fontFamily": "Segoe UI, Helvetica, Arial, sans-serif", "actorBkg": "#1e293b", "actorTextColor": "#f1f5f9", "actorBorder": "#475569", "signalColor": "#334155", "signalTextColor": "#0f172a", "noteBkgColor": "#fff7ed", "noteBorderColor": "#ea580c", "activationBkgColor": "#e0ecff", "sequenceNumberColor": "#ffffff"}}}%%
sequenceDiagram
    autonumber
    participant SIM as Simulator
    participant K as Kafka
    participant ING as quality_ingest
    participant ENR as defect_enrichment
    participant ERP as MySQL ERP
    participant WM as WireMock (classify/quarantine)
    participant PG as Postgres (quality)
    participant TEAMS as Teams
    participant JIRA as Jira
    participant SLA as qtp sla scan

    SIM->>K: publish inspection (defect_codes=[SUR-001]) → quality.inspections
    K->>ING: consume inspection
    Note over ING: header enrich · data-quality tag · schema_version stamp
    ING->>ING: Stream Selector → defect lane
    ING->>K: publish → quality.defects.major

    K->>ENR: consume defect
    ENR->>ERP: JDBC Lookup batch_id → supplier + grade + certificate
    ERP-->>ENR: supplier_id, supplier_name, material_grade, certificate_url
    ENR->>WM: POST /defects/classify (whole record)
    WM-->>ENR: {defect_category: CRITICAL, recommended_action: SCRAP}
    ENR->>ENR: flatten defect_codes → defect_codes_str (JS)
    ENR->>WM: PUT /quarantine/register (3 retries, exponential backoff)
    WM-->>ENR: {quarantine_id}
    ENR->>PG: INSERT defective_parts (+ quarantine_id, traceability_completed_at)

    par Severity routing (CRITICAL)
        ENR->>WM: audit alert (dispatcher fragment)
        ENR->>ENR: rollup gate: first in window? send : count
        ENR->>TEAMS: POST Adaptive Card (+N rolled up, if any)
        ENR->>JIRA: POST /rest/api/3/issue (ADF description)
        JIRA-->>ENR: 201 Created (QUAL-####)
    end

    Note over PG,SLA: continuous compliance loop
    SLA->>PG: scan defective_parts older than 2h without completed traceability
    alt breach risk
        SLA->>JIRA: escalate: create SLA breach ticket
        SLA->>TEAMS: escalate: CRITICAL alert
    else within SLA
        SLA-->>SLA: chain complete (part → batch → supplier → certificate)
    end
```

## ISO 9001 controls at each step

| Step | Control | Evidence |
|-----|---------|----------|
| schema_version stamp | Schema version on every record | `defective_parts.schema_version` |
| ERP lookup | Full traceability chain | Supplier, batch, and certificate columns |
| Classify | Deterministic severity and action | `defect_category`, `recommended_action` |
| Quarantine (3 retries) | Mandatory quarantine of CRITICAL and MAJOR parts | `quarantine_log`, `quarantine_id` |
| Rollup gate and routing | Severity-routed, rate-limited alerting | Teams card · Jira ticket · WireMock journal |
| SLA scan | Two-hour traceability SLA | `traceability_completed_at - inspection_timestamp` |

If any stage fails, the record goes to the MinIO dead-letter queue, where it can be seen and
recovered. A downstream outage cannot silently lose a defect.
