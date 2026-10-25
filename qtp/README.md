# QTP SDK package

`qtp` automates the Quality & Traceability Platform through the StreamSets Platform SDK. It is a
structured CLI, `qtp <group> <action>`, that builds, governs, operates, and monitors the
manufacturing pipelines and fragments from code instead of the UI.

## Install

```bash
pip install -e .          # from the repo root; provides the `qtp` command
# or, without installing:
python -m qtp.cli --help
```

## Credentials

Add your Control Hub credentials to `.env` in the repo root:

```
SCH_URL=...
SCH_CRED_ID=...
SCH_TOKEN=...
SDC_ID=...   # optional authoring Data Collector id
```

The package reads credentials from the environment only. None are hardcoded.

## Command tree

```
qtp fragments     build | list
qtp pipelines     build | validate | preview | generate | add-line | promote | export
qtp connections   create | list
qtp jobs          status | create | start | stop | update-parameters
qtp sla           check | scan
qtp monitor       defect-rate
qtp report        shift
qtp preflight     run
qtp demo          run | seed
qtp evidence      pack
qtp schedule      run | install
qtp metrics       serve
qtp teardown      run
```

Run `qtp <group> --help` for the options of each command. The root [README](../README.md)
describes what each group does.

## Owned fragments

Build and publish the two fragments this package owns:

```bash
qtp fragments build                       # both
qtp fragments build --name s3_partition_formatter
```

- **`s3_partition_formatter`** is an Expression Evaluator that stamps a standard
  `/_partition_path` (`${OUTPUT_BASE_PATH}/year/month/day/shift/line_id/`).
- **`dead_letter_queue_handler`** is an Expression Evaluator that only adds error metadata
  (`dlq_source_pipeline`, `dlq_error_stage`, `dlq_error_message`, `dlq_error_code`,
  `dlq_error_timestamp`) and works with any source. It has no destination: the pipeline that
  uses it owns the DLQ sink and its named MinIO connection.

Both fragments contain logic only, with no hardcoded values. Endpoints, credentials, buckets, and
connections belong to the pipeline that uses them.

## Layout

```
qtp/
  cli.py          # command tree
  common/         # Control Hub session, config, connections, SDK compatibility
  fragments/      # fragment builders and registry
  pipelines/      # pipeline builders (ingest, enrichment, archive, DLQ recovery, audit)
  manage/         # per-line generation, promotion, export
  jobs/           # Control Hub job lifecycle and runtime parameters
  sla/            # traceability chain and two-hour SLA scan
  monitor/        # defect-rate check
  report/         # shift report
  metrics/        # Prometheus exporter
  demo/           # demo runner, seed, preflight, evidence, schedule, teardown
config/
  lines.yaml      # production lines used for pipeline generation
  env_map.yaml    # dev-to-prod connection mapping used for promotion
  fragment_pins.yaml
```
