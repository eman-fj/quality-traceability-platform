# QTP SDK package

StreamSets Platform SDK automation for the Quality & Traceability Platform.
A structured CLI — `qtp <group> <action>` — that builds, governs, and monitors the
manufacturing pipelines and fragments via code, not the UI.

## Install
```bash
pip install -e .          # from the repo root; gives the `qtp` command
# or, without installing:
python -m qtp.cli --help
```

## Credentials
Add your Control Hub credentials to `.env` (reuse the values from `D:\streamsets\SDK\.env`):
```
SCH_URL=...
SCH_CRED_ID=...
SCH_TOKEN=...
SDC_ID=...   # optional authoring Data Collector id
```
Credentials are read from the environment only — never hardcoded.

## Command tree
```
qtp fragments build [--name NAME] [--engine-id ID] [--draft]   # implemented
qtp fragments list                                             # implemented
qtp pipelines generate|add-line|promote|deploy                 # planned (Day 4-6)
qtp jobs status|start|stop                                     # planned (Day 6)
qtp preflight run                                              # planned (Day 7)
qtp sla check                                                  # planned (Day 7)
qtp report shift                                               # planned (Day 8)
qtp evidence pack                                              # planned (Day 8)
qtp demo run                                                   # planned (Day 8)
qtp teardown run                                               # planned (Day 9)
```

## Owned fragments (Day 3)
Build and publish your two owned fragments to `kermit-accelerator-dev`:
```bash
qtp fragments build                       # both
qtp fragments build --name s3_partition_formatter
```
- **`s3_partition_formatter`** — Expression Evaluator that stamps a standardised
  `/_partition_path` (`${OUTPUT_BASE_PATH}/year/month/day/shift/line_id/`).
- **`dead_letter_queue_handler`** — enrichment-only Expression Evaluator that stamps
  source-agnostic error metadata (`dlq_source_pipeline`, `dlq_error_stage`,
  `dlq_error_message`, `dlq_error_code`, `dlq_error_timestamp`). No destination — the
  consuming pipeline owns the DLQ sink + its named MinIO connection (like UC-7).

Both fragments are pure logic with no hardcoded values; environment-specifics
(endpoints, credentials, buckets, connections) live in the consuming pipeline.

## Layout
```
qtp/
  cli.py                 # command tree
  common/                # controlhub session, config loader, utils
  fragments/             # owned-fragment builders + registry
config/
  lines.yaml             # production lines (pipeline generation, Day 4+)
  env_map.yaml           # dev->prod connection mapping (promotion, Day 6)
```
