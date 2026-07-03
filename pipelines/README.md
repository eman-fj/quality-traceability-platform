# Exported StreamSets pipelines (importable into Control Hub)

This folder holds the exported pipeline definitions — a required deliverable (brief §7).

| File | What it is |
|------|------------|
| `manufacturing__<flow>.json` | One pretty-printed JSON per published pipeline (`pipelineConfig` + `pipelineRules` + `libraryDefinitions`). Human-reviewable. |
| `manufacturing__pipelines.zip` | The combined export archive — **import this** into Control Hub (UI: *Import Pipelines*, or `sch.import_pipelines_from_archive`). |

## Regenerate

```bash
qtp pipelines export                 # all manufacturing__* pipelines -> pipelines/
qtp pipelines export --name manufacturing__quality_ingest manufacturing__defect_enrichment
qtp pipelines export --no-archive    # JSON only, skip the .zip
```

The export reads the latest published version of each `manufacturing__*` pipeline from Control
Hub (read-only — nothing is created or changed). Credentials come from `.env`.
