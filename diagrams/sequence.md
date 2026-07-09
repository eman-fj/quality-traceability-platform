# QTP: Sequence Diagram: one CRITICAL defect, end to end

Traces a single CRITICAL part from inspection to a closed 2-hour traceability record,
across every system it touches. This is the "golden thread" the demo and evidence pack
reproduce deterministically.

```mermaid
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
    ENR->>WM: PUT /quarantine/register (retry ×3 exp-backoff)
    WM-->>ENR: {quarantine_id}
    ENR->>PG: INSERT defective_parts (+ quarantine_id, traceability_completed_at)

    par Severity fan-out (CRITICAL)
        ENR->>WM: audit alert (dispatcher fragment)
        ENR->>ENR: rollup gate - first in window? emit : count
        ENR->>TEAMS: POST Adaptive Card (+N rolled-up if any)
        ENR->>JIRA: POST /rest/api/3/issue (ADF description)
        JIRA-->>ENR: 201 Created (QUAL-####)
    end

    Note over PG,SLA: continuous compliance loop
    SLA->>PG: scan defective_parts past 2h w/o completed traceability
    alt breach risk
        SLA->>JIRA: escalate - create SLA-breach ticket
        SLA->>TEAMS: escalate - CRITICAL alert
    else within SLA
        SLA-->>SLA: traceability chain complete (part → batch → supplier → certificate)
    end
```

## What each hop guarantees (ISO 9001 controls)

| Hop | Control | Evidence |
|-----|---------|----------|
| schema_version stamp | Schema version on every record | `defective_parts.schema_version` |
| ERP lookup | Full traceability chain | supplier + batch + certificate columns |
| classify | Deterministic severity → action | `defect_category`, `recommended_action` |
| quarantine (retry ×3) | Mandatory quarantine of CRITICAL/MAJOR | `quarantine_log`, `quarantine_id` |
| rollup gate + fan-out | Severity-routed alerting, rate-capped | Teams card · Jira ticket · WireMock journal |
| SLA scan | 2-hour traceability SLA | `traceability_completed_at - inspection_timestamp` |

Any stage failure routes the record to the MinIO **dead-letter queue** (visible, recoverable)
rather than dropping it - so a downstream outage never silently loses a defect.
