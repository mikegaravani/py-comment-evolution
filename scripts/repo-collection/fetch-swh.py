"""
Fetch/materialize Software Heritage directory trees based on snapshots.csv.

Inputs:
  - data/metadata/snapshots.csv

Outputs:
  - data/raw/software_heritage/<repo>/<release>/...
  - data/metadata/provenance.jsonl
  - data/raw/software_heritage/<repo>/<release>/_MANIFEST.json

USAGE:
  python scripts/repo-collection/fetch-swh.py
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

SWH_API = "https://archive.softwareheritage.org/api/1"

SNAPSHOTS_CSV = Path("data/metadata/snapshots.csv")
OUT_ROOT = Path("data/raw/software_heritage")
PROVENANCE_JSONL = Path("data/metadata/provenance.jsonl")

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "thesis-swh-fetch",
        "Accept": "application/json",
    }
)

DEFAULT_TIMEOUT = 60

class FetchError(RuntimeError):
    pass

@dataclass
class SnapshotRow:
    name: str
    group: str
    origin_url: str
    release: str
    tag_ref: str
    snapshot_id: str
    revision_id: str
    directory_id: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def http_get(url: str, params: Optional[dict] = None, retries: int = 6) -> requests.Response:
    last_err = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            if r.status_code == 429:
                # rate limiting
                time.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(1.0 * (attempt + 1))
    raise FetchError(f"GET failed after {retries} retries: {url} ({last_err})")


def http_get_json(url: str, params: Optional[dict] = None) -> Any:
    return http_get(url, params=params).json()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_relpath(rel: str) -> str:
    # prevent directory traversal surprises
    rel = rel.replace("\\", "/")
    while rel.startswith("/"):
        rel = rel[1:]
    if ".." in Path(rel).parts:
        raise FetchError(f"Unsafe relative path from API: {rel}")
    return rel


def read_snapshots_csv(path: Path) -> List[SnapshotRow]:
    if not path.exists():
        raise FetchError(f"Missing snapshots CSV: {path}")

    rows: List[SnapshotRow] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            # skip incomplete rows (e.g., resolution failures)
            if not row.get("directory_id"):
                continue
            rows.append(
                SnapshotRow(
                    name=row["name"],
                    group=row["group"],
                    origin_url=row["origin_url"],
                    release=row["release"],
                    tag_ref=row.get("tag_ref", ""),
                    snapshot_id=row.get("snapshot_id", ""),
                    revision_id=row.get("revision_id", ""),
                    directory_id=row["directory_id"],
                )
            )
    return rows


def list_directory_entries(dir_id: str) -> List[dict]:
    """
    GET /api/1/directory/<dir_id>/
    Response can be:
      - list of entries (common)
      - dict with 'entries' or 'content' key (also seen)
    """
    url = f"{SWH_API}/directory/{dir_id}/"
    data = http_get_json(url)

    # Case 1: API returns a plain list of entries
    if isinstance(data, list):
        return data

    # Case 2: API returns an object containing entries
    if isinstance(data, dict):
        if isinstance(data.get("entries"), list):
            return data["entries"]
        if isinstance(data.get("content"), list):
            return data["content"]

    raise FetchError(f"Unexpected directory response shape for {dir_id}: {type(data)}")


def content_raw_bytes(content_id: str) -> bytes:
    """
    GET /api/1/content/sha1_git:<hash>/raw/
    """
    url = f"{SWH_API}/content/sha1_git:{content_id}/raw/"
    r = http_get(url)
    return r.content


def write_provenance(record: Dict[str, Any]) -> None:
    PROVENANCE_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with PROVENANCE_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_manifest_hash(file_paths: List[str]) -> str:
    """
    Deterministic hash over the list of relative file paths.
    """
    h = hashlib.sha256()
    for p in sorted(file_paths):
        h.update(p.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def materialize_tree(
    dir_id: str,
    out_dir: Path,
    *,
    sleep_s: float = 0.0,
) -> Tuple[int, int, List[str]]:
    """
    Recursively materialize directory <dir_id> under out_dir.

    Returns: (num_files_written, num_files_skipped, relative_paths_list)
    """
    written = 0
    skipped = 0
    rel_paths: List[str] = []

    stack: List[Tuple[str, Path]] = [(dir_id, out_dir)]

    while stack:
        current_dir_id, current_out = stack.pop()
        entries = list_directory_entries(current_dir_id)

        for e in entries:
            name = e.get("name")
            typ = e.get("type")
            target = e.get("target")

            if not isinstance(name, str) or not isinstance(typ, str) or not isinstance(target, str):
                continue

            # names are bytes-ish sometimes; SWH returns strings
            # Guard: clean path components
            name = name.replace("/", "_")

            if typ == "dir":
                stack.append((target, current_out / name))
                continue

            if typ == "file":
                rel = os.path.relpath((current_out / name), out_dir)
                rel = safe_relpath(rel)
                rel_paths.append(rel)

                out_path = current_out / name
                if out_path.exists() and out_path.is_file() and out_path.stat().st_size > 0:
                    skipped += 1
                    continue

                ensure_parent(out_path)
                data = content_raw_bytes(target)
                out_path.write_bytes(data)
                written += 1

                if sleep_s:
                    time.sleep(sleep_s)

                continue

            # ignore: rev, rel, etc. (rare in directory entries)
            # you can log these later if you want

    return written, skipped, rel_paths


def main() -> None:
    rows = read_snapshots_csv(SNAPSHOTS_CSV)
    if not rows:
        raise FetchError(f"No resolvable rows found in {SNAPSHOTS_CSV}")

    for row in rows:
        out_dir = OUT_ROOT / row.name / row.release
        out_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = out_dir / "_MANIFEST.json"

        print(f"\nFetching {row.name} release={row.release}")
        print(f"  directory_id={row.directory_id}")
        print(f"  -> {out_dir}")

        start = time.time()
        status = "success"
        err_msg = None

        try:
            written, skipped, rel_paths = materialize_tree(
                row.directory_id,
                out_dir,
                sleep_s=0.0,
            )

            manifest = {
                "repo": row.name,
                "release": row.release,
                "origin_url": row.origin_url,
                "tag_ref": row.tag_ref,
                "snapshot_id": row.snapshot_id,
                "revision_id": row.revision_id,
                "directory_id": row.directory_id,
                "retrieved_at": utc_now_iso(),
                "file_count": len(rel_paths),
                "files_written": written,
                "files_skipped": skipped,
                "manifest_paths_sha256": compute_manifest_hash(rel_paths),
            }
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

            prov = {
                **manifest,
                "duration_seconds": round(time.time() - start, 3),
                "status": "success",
            }
            write_provenance(prov)

            print(f"  OK: files={len(rel_paths)} written={written} skipped={skipped}")
            print(f"  manifest: {manifest_path}")

        except Exception as e:
            status = "error"
            err_msg = str(e)

            prov = {
                "repo": row.name,
                "release": row.release,
                "origin_url": row.origin_url,
                "tag_ref": row.tag_ref,
                "snapshot_id": row.snapshot_id,
                "revision_id": row.revision_id,
                "directory_id": row.directory_id,
                "retrieved_at": utc_now_iso(),
                "status": status,
                "error": err_msg,
            }
            write_provenance(prov)

            print(f"  ERROR: {err_msg}")

    print("\nDone.")


if __name__ == "__main__":
    main()
