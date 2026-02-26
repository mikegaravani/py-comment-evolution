from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class SnapshotRow:
    name: str
    group: str
    origin_url: str
    release: str
    release_date: str
    tag_ref: str
    snapshot_id: str
    revision_id: str
    directory_id: str


def load_snapshots_csv(path: Path) -> list[SnapshotRow]:
    """
    Load data/metadata/snapshots.csv as typed rows.
    """
    rows: list[SnapshotRow] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "name",
            "group",
            "origin_url",
            "release",
            "release_date",
            "tag_ref",
            "snapshot_id",
            "revision_id",
            "directory_id",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"snapshots.csv missing columns: {sorted(missing)}")

        for r in reader:
            rows.append(
                SnapshotRow(
                    name=(r.get("name") or "").strip(),
                    group=(r.get("group") or "").strip(),
                    origin_url=(r.get("origin_url") or "").strip(),
                    release=(r.get("release") or "").strip(),
                    release_date=(r.get("release_date") or "").strip(),
                    tag_ref=(r.get("tag_ref") or "").strip(),
                    snapshot_id=(r.get("snapshot_id") or "").strip(),
                    revision_id=(r.get("revision_id") or "").strip(),
                    directory_id=(r.get("directory_id") or "").strip(),
                )
            )
    return rows


def resolve_snapshot_root(raw_root: Path, row: SnapshotRow) -> Optional[Path]:
    """
    Try to find the materialized snapshot directory for a row, and return path.
      data/raw/software_heritage/<name>/<release>/
    """
    candidates: list[Path] = []

    if row.release:
        candidates.append(raw_root / row.name / row.release)

    # if we put snapshot directory into the name directory
    candidates.append(raw_root / row.name)

    for c in candidates:
        if c.exists() and c.is_dir():
            return c

    return None


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def log_jsonl(log_path: Path, record: dict) -> None:
    """
    append a JSONL record
    """
    ensure_dir(log_path.parent)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_manifest(snapshot_root: Path) -> Optional[dict]:
    """
    Read <snapshot_root>/_MANIFEST.json if present, else None.
    Returns parsed JSON dict or None on missing/parse errors.
    """
    manifest_path = snapshot_root / "_MANIFEST.json"
    if not manifest_path.exists():
        return None
    try:
        with manifest_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def manifest_status_is_success(snapshot_root: Path) -> bool:
    """
    True iff _MANIFEST.json exists and has {"status": "success"}.
    AKA the repo is completely materialized and ready for processing.
    """
    m = read_manifest(snapshot_root)
    return bool(m) and (m.get("status") == "success")