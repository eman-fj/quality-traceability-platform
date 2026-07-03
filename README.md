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

```bash
# 1. (optional) override any clashing host ports / creds
cp .env.example .env

# 2. boot the whole environment (core + manufacturing specialist stack + simulator)
docker compose up -d --build

# 3. watch inspection events flow (CRITICAL defects injected on a timer)
docker compose logs -f simulator
```

| Service | URL / port | Login |
|---------|-----------|-------|
| Grafana | http://localhost:3000 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Mailhog UI | http://localhost:8025 | — |
| WireMock admin | http://localhost:8080/__admin | — |
| Prometheus | http://localhost:9090 | — |
| Kafka | localhost:9092 | — |
| Postgres (`quality`) | localhost:5432 | qtp / qtp_pw |
| MySQL (`erp`) | localhost:3306 | qtp / qtp_pw |

**What boots:** Kafka (+ 3 topics auto-created), Postgres (`quality` schema), MySQL ERP
(suppliers + material_batches seeded), WireMock (classify + quarantine stubs), MinIO (4 buckets
auto-created), Mailhog, Prometheus, Grafana, and the inspection simulator producing events for
all three lines.

```bash
docker compose down        # stop
docker compose down -v     # stop + wipe data volumes
```

---

## One-command demo

The entire ISO 9001 story runs from a single command. It is **deterministic and repeatable**
— it seeds a fixed "golden thread" scenario into the warehouse and runs the real compliance
scripts against it, so it tells an identical, complete story every time, with or without a live
Control Hub engine.

```bash
qtp demo run                 # preflight -> seed -> monitor -> SLA -> report -> evidence -> summary
```

On Windows, one script also boots the stack, waits for health, runs the demo, and opens the
evidence pack:

```powershell
./scripts/demo.ps1           # or:  make demo   (Linux/macOS)
```

What `qtp demo run` does, as eight narrated stages:

1. **Preflight** — GREEN/RED health board for all nine services.
2. **Seed** — the golden thread: one CRITICAL part traced to a SUSPENDED supplier (SLA *met*),
   one part breaching the 2-hour SLA, a line pushed over the 2% defect ceiling, and a full
   shift dataset. All rows are namespaced `DEMO-*` and idempotent — they never touch real data.
3. **Defect-rate monitor** — fires a CRITICAL alert + Jira ticket for the hot line.
4. **SLA scan** — escalates every part untraced past two hours.
5. **SLA chain** — reconstructs part → batch → supplier → certificate for the hero part.
6. **Shift report** — Excel to MinIO + email to Mailhog.
7. **Evidence pack** — `evidence/pack-<ts>/evidence.{json,html}` (screenshot-ready audit report).
8. **Compliance result** — each of the five ISO 9001 controls resolved to PASS/FAIL, with the evidence pack path.

Opt-in **live overlays** layer real pipeline activity on top of the deterministic story:

```bash
qtp demo run --inject-critical   # publish a real CRITICAL inspection event to Kafka
qtp demo run --start-jobs        # start the Control Hub jobs (verify SDK job API on your engine)
```

## Scheduling the compliance cadence

The compliance controls are recurring duties, not one-shots. Run them on a schedule:

```bash
qtp schedule run                 # in-process: monitor (5m) + SLA scan (15m) + shift reports
qtp schedule install             # write cron / Windows Task Scheduler / docker-compose artefacts
```

`qtp schedule install` writes ready-to-use artefacts to `scripts/scheduled/` (a crontab, the
Windows `schtasks` commands, and a `qtp-scheduler` compose service) so the cadence installs the
native way on any host.

## Runbooks

The three runbooks required by the brief — all driven by the SDK CLI:

- [x] **Inject a CRITICAL defect and trace it through the full pipeline**
  — `qtp demo run --inject-critical`, then `qtp sla check --part <id>`.
- [x] **Verify the traceability chain in Postgres and MySQL**
  — `qtp sla check --part DEMO-CRIT-0001` prints the full part→batch→supplier→certificate chain.
- [x] **Trigger the 2-hour SLA alert with the traceability checker**
  — `qtp sla scan` (the seeded `DEMO-SLA-0002` part breaches the 2-hour window).

## SDK (QTP CLI)

Structured Python package — run `qtp --help` for the full command tree.

| Group | Example | Purpose |
|-------|---------|---------|
| `fragments` | `qtp fragments build` / `list` | Build, publish, and list shared fragments |
| `pipelines` | `qtp pipelines generate` | Generate per-line ingestion + defect pipelines from `lines.yaml` |
| `pipelines` | `qtp pipelines add-line --id line_4` | Onboard a new production line |
| `pipelines` | `qtp pipelines promote --name <p> --to prod` | Environment promotion via env_map |
| `pipelines` | `qtp pipelines export` | Export published pipelines to importable JSON in `/pipelines` |
| `sla` | `qtp sla check --part <id>` / `scan` | Traceability + 2-hour SLA enforcement |
| `monitor` | `qtp monitor defect-rate` | Per-line defect-rate compliance check |
| `report` | `qtp report shift --shift A --date <d>` | Shift quality report (Excel → MinIO + Mailhog) |
| `preflight` | `qtp preflight run` | Service health board |
| `demo` | `qtp demo run` | **One-command end-to-end compliance demonstration** |
| `evidence` | `qtp evidence pack` | Compliance evidence pack (JSON + HTML audit report) |
| `schedule` | `qtp schedule run` / `install` | Recurring compliance cadence |
| `jobs` | `qtp jobs status` / `create` / `start` / `stop` | Control Hub job lifecycle (the live path) |
| `teardown` | `qtp teardown run` | Remove the demo's `DEMO-*` footprint |

---

## Fragments

All ten shared fragments are used or explicitly accounted for — see the
[Fragment Register & Applicability Matrix](docs/fragment-register.md).

## Status

| Deliverable | State |
|-------------|-------|
| Repo + skeleton | ✅ Day 1 |
| Architecture diagram + ERD | ✅ Day 1 (draft) |
| docker-compose.yml + simulator | ✅ Day 2 |
| SDK package (`qtp` CLI) + owned fragments | ✅ |
| Pipelines (11 built) | ✅ |
| Compliance control tower (SLA, defect-rate, audit, schema) | ✅ |
| Shift report + evidence pack | ✅ |
| One-command demo + scheduler | ✅ |
| Dashboards (Grafana) | 🔄 |
| Slide deck + LinkedIn | ⬜ |
