"""pipeline_export.py - export published pipelines to importable JSON.

Required deliverable (brief §7 / Master doc §5.1): every StreamSets pipeline exported as JSON
into ``/pipelines``, importable directly into Control Hub. This pulls each published
``manufacturing__*`` pipeline from Control Hub via the SDK and writes:

  * ``pipelines/<name>.json`` - one pretty-printed file per pipeline (exported per-pipeline so
    each file is named from the pipeline object itself, not a guessed archive entry), and
  * ``pipelines/manufacturing__pipelines.zip`` - the combined export archive, the artifact you
    import back into Control Hub in one shot.

Read-only against Control Hub (``sch.export_pipelines`` returns the export bytes; nothing is
created or changed). Credentials come from .env via ``qtp.common.controlhub``.
"""

import io
import json
import zipfile
from pathlib import Path

from ..common.controlhub import get_control_hub
from ..common.util import get_logger

log = get_logger()

PREFIX = "manufacturing__"
ARCHIVE_NAME = "manufacturing__pipelines.zip"


def _version(pipeline) -> int:
    try:
        return int(getattr(pipeline, "version", 0) or 0)
    except (TypeError, ValueError):
        return 0


def select_pipelines(sch, names=None, prefix=PREFIX) -> dict:
    """Latest published, non-fragment pipeline per name under ``prefix``.

    The shared workspace can hold duplicate-name copies (different versions); keep the
    highest version of each so the export is unambiguous.
    """
    selected: dict = {}
    for pipeline in sch.pipelines.get_all():
        if getattr(pipeline, "fragment", False):
            continue
        name = getattr(pipeline, "name", None)
        if not name or not name.startswith(prefix):
            continue
        if names and name not in names:
            continue
        current = selected.get(name)
        if current is None or _version(pipeline) >= _version(current):
            selected[name] = pipeline
    return selected


def _pretty(raw: bytes) -> bytes:
    """Pretty-print exported JSON for readability; pass through if it isn't JSON."""
    try:
        return json.dumps(json.loads(raw), indent=2).encode("utf-8")
    except (ValueError, TypeError):
        return raw


def _first_json(archive: bytes) -> bytes | None:
    with zipfile.ZipFile(io.BytesIO(archive)) as zf:
        for entry in zf.namelist():
            if entry.endswith(".json"):
                return zf.read(entry)
    return None


def export(out_dir: str = "pipelines", names=None, prefix: str = PREFIX,
           save_archive: bool = True) -> list[str]:
    """Export the selected published pipelines to JSON (+ a combined importable archive)."""
    sch = get_control_hub()
    selected = select_pipelines(sch, names, prefix)
    if not selected:
        raise RuntimeError(
            f"No published pipelines found with prefix '{prefix}'. "
            f"Build/publish them first (qtp pipelines build / generate)."
        )

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: list[str] = []

    # Per-pipeline export so each file is named from the pipeline itself.
    for name, pipeline in sorted(selected.items()):
        raw = _first_json(sch.export_pipelines([pipeline]))
        if raw is None:
            log.warning("Export for '%s' contained no JSON; skipped.", name)
            continue
        path = out / f"{name}.json"
        path.write_bytes(_pretty(raw))
        written.append(path.name)
        log.info("Exported %s", path)

    # Combined archive: re-import every pipeline into Control Hub in one operation.
    if save_archive:
        archive = sch.export_pipelines(list(selected.values()))
        (out / ARCHIVE_NAME).write_bytes(archive)
        log.info("Wrote importable archive: %s", out / ARCHIVE_NAME)

    log.info("Exported %d pipeline JSON file(s) to %s/", len(written), out)
    print(f"\n  Exported {len(written)} pipeline(s) to {out}/:")
    for name in written:
        print(f"    - {name}")
    if save_archive:
        print(f"    - {ARCHIVE_NAME}  (import this archive back into Control Hub)\n")
    return written


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="pipeline-export",
                                     description="Export published pipelines to importable JSON.")
    parser.add_argument("--name", nargs="*", help="full pipeline name(s) (default: all manufacturing__*)")
    parser.add_argument("--out", default="pipelines", help="output directory (default: pipelines/)")
    parser.add_argument("--no-archive", action="store_true", help="skip the combined importable .zip")
    args = parser.parse_args(argv)
    try:
        written = export(out_dir=args.out, names=args.name, save_archive=not args.no_archive)
        return 0 if written else 1
    except Exception as exc:  # noqa: BLE001
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
