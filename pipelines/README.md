# Exported StreamSets pipelines

This folder holds the exported pipeline definitions, ready to import into Control Hub.

| File | Contents |
|------|----------|
| `manufacturing__<flow>.json` | One formatted JSON file per published pipeline (`pipelineConfig`, `pipelineRules`, and `libraryDefinitions`), readable in a code review |
| `manufacturing__pipelines.zip` | The combined export archive. Import this one into Control Hub, through *Import Pipelines* in the UI or `sch.import_pipelines_from_archive` |

## Regenerate

```bash
qtp pipelines export                 # all manufacturing__* pipelines -> pipelines/
qtp pipelines export --name manufacturing__quality_ingest manufacturing__defect_enrichment
qtp pipelines export --no-archive    # JSON only, skip the .zip
```

The export reads the latest published version of each `manufacturing__*` pipeline from Control
Hub. It is read-only and changes nothing there. Credentials come from `.env`.
