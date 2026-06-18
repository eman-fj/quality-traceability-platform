# Quality & Traceability Platform (QTP)

**Production Line Quality Control & Supplier Traceability**
Kermit StreamSets Engineering Accelerator — Manufacturing | Syeda Eman Fatima 

---

## Vision

> QTP turns three production lines of raw inspection events into a real-time quality
> command centre that traces any defective part back to its supplier and certificate in
> under two hours — automatically, auditably, and provably ISO 9001 compliant.

### Value statements

- **Business** — Replaces a 6-hour manual traceability process with sub-2-hour automated
  traceability, containing defects before they ship and protecting against EU
  product-liability and ISO 9001 compliance risk.
- **Technical** — A governed, SDK-automated StreamSets platform: single-responsibility
  pipelines, programmatic fragment application, dev→prod promotion, and self-validating
  health checks that onboard a new production line with one command.
- **Compliance / governance** — Every part is schema-versioned and audited; every defect is
  classified, quarantined, and traced within an enforced SLA; every breach raises a
  severity-routed alert and an auditable evidence pack.
- **Demo** — A single critical defect is born, caught, enriched, traced to its supplier,
  quarantined, alerted, and reported — live, in under ten minutes.

---

## Architecture

QTP is a six-layer platform. See [`diagrams/architecture.md`](diagrams/architecture.md) for the
full diagram and [`diagrams/erd.md`](diagrams/erd.md) for the data model.

1. **Factory Event Backbone** — Kafka inspection events from three lines.
2. **Quality Intelligence Layer** — StreamSets ingestion, routing, enrichment, DLQ.
3. **Traceability / Supplier Intelligence** — part→batch→supplier→certificate digital thread.
4. **Compliance Control Tower** — 2-hour SLA, quarantine, schema versioning, audit, alerting.
5. **Reporting & Executive Observability** — Grafana command centre + shift quality report.
6. **Demo & Evidence Layer** — demo orchestrator + auto-generated audit evidence packs.

---

## Repository layout

| Path | Contents |
|------|----------|
| `/pipelines` | Exported StreamSets pipeline JSON (importable into Control Hub) |
| `/qtp` | Python SDK package (CLI: `qtp <group> <action>`) + `requirements.txt` |
| `/diagrams` | Architecture diagram, ERD, sequence diagram (Mermaid + rendered) |
| `/docs` | Fragment register, runbooks, governance docs, talking points |
| `/evidence` | Screenshots, logs, reports, evidence packs — captured as work proceeds |

---

## Quickstart (60 seconds)

> _TODO (Day 2): `docker-compose up` → all services healthy → run the inspection simulator._

```
# placeholder — to be completed when docker-compose lands
```

## Runbooks

> The three runbooks below are required by the brief. _TODO — write each as its feature lands._

- [ ] **Inject a CRITICAL defect and trace it through the full pipeline** (Day 5–7)
- [ ] **Verify the traceability chain in Postgres and MySQL** (Day 5)
- [ ] **Trigger the 2-hour SLA alert with the traceability checker** (Day 7)

## SDK (QTP CLI)

> Structured Python package. _TODO — see [`docs/sdk-plan.md`](docs/sdk-plan.md) (to be added)._

| Group | Example | Purpose |
|-------|---------|---------|
| `pipelines` | `qtp pipelines generate --config lines.yaml` | Generate/deploy pipelines from config |
| `pipelines` | `qtp pipelines add-line --id line_4` | Onboard a new production line |
| `pipelines` | `qtp pipelines promote --from dev --to prod` | Environment promotion via env_map |
| `jobs` | `qtp jobs status` | Job lifecycle + status board |
| `sla` | `qtp sla check --part <id>` | Traceability + 2-hour SLA enforcement |
| `report` | `qtp report shift --shift A --date <d>` | Shift quality report |
| `evidence` | `qtp evidence pack --part <id>` | Per-part audit evidence pack |
| `demo` | `qtp demo run` | End-to-end demo orchestrator |

---

## Fragments

All ten shared fragments are used or explicitly accounted for — see the
[Fragment Register & Applicability Matrix](docs/fragment-register.md).

## Status

| Deliverable | State |
|-------------|-------|
| Repo + skeleton | ✅ Day 1 |
| Architecture diagram + ERD | ✅ Day 1 (draft) |
| docker-compose.yml | ⬜ |
| Pipelines (4) | ⬜ |
| SDK package | ⬜ |
| Compliance control tower | ⬜ |
| Dashboards + reports | ⬜ |
| Slide deck + LinkedIn | ⬜ |
