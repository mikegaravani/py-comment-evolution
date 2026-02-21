"""
Fetch/materialize Software Heritage directory trees based on snapshots.csv.

Inputs:
  - data/metadata/snapshots.csv

Outputs:
  - data/raw/software_heritage/<repo>/<release>/...
  - data/metadata/provenance.jsonl
  - data/raw/software_heritage/<repo>/<release>/_MANIFEST.json

USAGE:
 -> python scripts/repo-collection/fetch-swh.py


EXTRA:
Useful command to check rate limiting details (eats 1 call if available):
 -> curl -i https://archive.softwareheritage.org/api/1/directory/f41c2246cb66fe1aac134195fe788bd72baf2853/ | head -n 20
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
from typing import Any, Dict, List, Optional, Tuple
import random
import sys

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


def sleep_with_countdown(seconds: float, prefix: str = "Sleeping") -> None:
    remaining = int(seconds)
    while remaining > 0:
        mins, secs = divmod(remaining, 60)
        sys.stdout.write(f"\r{prefix}: {mins:02d}:{secs:02d} ")
        sys.stdout.flush()
        time.sleep(1)
        remaining -= 1
    sys.stdout.write("\r" + " " * 40 + "\r")  # clear line
    sys.stdout.flush()


def http_get(url: str, params: Optional[dict] = None, retries: int = 10) -> requests.Response:
    last_err: Optional[Exception] = None
    last_status: Optional[int] = None
    last_text: Optional[str] = None

    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            last_status = r.status_code

            if r.status_code == 429:
                # Rate limiting headers
                reset = r.headers.get("X-Ratelimit-Reset")
                remaining = r.headers.get("X-Ratelimit-Remaining")
                limit = r.headers.get("X-Ratelimit-Limit")

                sleep_s: float

                if reset:
                    try:
                        reset_ts = int(reset)
                        now_ts = int(time.time())
                        sleep_s = max(1.0, (reset_ts - now_ts) + 2.0)
                    except ValueError:
                        sleep_s = min(300.0, 2.0 ** attempt)
                else:
                    sleep_s = min(300.0, 2.0 ** attempt)

                sleep_s += random.random() * 1.0

                print(
                    f"  RETRY {attempt+1}/{retries} 429 for {url} "
                    f"(limit={limit}, remaining={remaining}, reset={reset}) -> sleeping {sleep_s:.1f}s"
                )
                sleep_with_countdown(sleep_s, prefix="  Rate limited, waiting")
                continue

            if 500 <= r.status_code < 600:
                last_text = (r.text or "")[:500]
                backoff = min(60.0, 1.5 ** attempt)
                time.sleep(backoff + random.random() * 0.25)
                continue

            r.raise_for_status()
            return r

        except Exception as e:
            last_err = e
            sleep_s = min(15.0, 1.25 ** attempt) + random.random() * 0.25
            print(f"  RETRY {attempt+1}/{retries} EXC {type(e).__name__} for {url} -> sleeping {sleep_s:.2f}s")
            time.sleep(sleep_s)

    detail = []
    if last_status is not None:
        detail.append(f"last_status={last_status}")
    if last_text:
        detail.append(f"last_body={last_text!r}")
    if last_err is not None:
        detail.append(f"last_err={last_err!r}")

    suffix = ("; " + ", ".join(detail)) if detail else ""
    raise FetchError(f"GET failed after {retries} retries: {url}{suffix}")



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
            # skip incomplete rows (for resolution failures)
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
) -> Tuple[int, int, List[str], List[dict]]:
    """
    Recursively materialize directory <dir_id> under out_dir.

    Returns: (written, skipped, rel_paths, failures)
    """
    written = 0
    skipped = 0
    rel_paths: List[str] = []
    failures: List[dict] = []

    stack: List[Tuple[str, Path]] = [(dir_id, out_dir)]

    while stack:
        current_dir_id, current_out = stack.pop()
        try:
            entries = list_directory_entries(current_dir_id)
        except Exception as e:
            failures.append({"dir_id": current_dir_id, "error": str(e)})
            print(f"  WARN: failed to list directory {current_dir_id}: {e}")
            continue


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

                try:
                    data = content_raw_bytes(target)
                    out_path.write_bytes(data)
                    written += 1
                except Exception as ex:
                    failures.append(
                        {
                            "type": "file",
                            "path": rel,
                            "content_id": target,
                            "error": str(ex),
                        }
                    )
                    print(f"  WARN: failed to fetch file {rel} content={target}: {ex}")
                    # keep going
                    continue

                if sleep_s:
                    time.sleep(sleep_s)

                continue

    return written, skipped, rel_paths, failures

def load_manifest(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def is_complete(out_dir: Path, manifest: Optional[dict]) -> bool:
    """
    Decide if <out_dir> is already fully materialized.
    """
    if not manifest:
        return False
    if manifest.get("status") != "success":
        return False

    return True


def main() -> None:
    rows = read_snapshots_csv(SNAPSHOTS_CSV)
    if not rows:
        raise FetchError(f"No resolvable rows found in {SNAPSHOTS_CSV}")

    for row in rows:
        out_dir = OUT_ROOT / row.name / row.release
        out_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = out_dir / "_MANIFEST.json"

        existing = load_manifest(manifest_path)
        if is_complete(out_dir, existing):
            print(f"\nSkipping {row.name} release={row.release} (already FULLY materialized)")
            continue

        print(f"\nFetching {row.name} release={row.release}")
        print(f"  directory_id={row.directory_id}")
        print(f"  -> {out_dir}")

        start = time.time()
        status = "success"
        err_msg = None

        try:
            written, skipped, rel_paths, failures = materialize_tree(
                row.directory_id,
                out_dir,
                sleep_s=0.0,
            )

            status = "success" if not failures else "partial_success"

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
                "directory_failures": failures,
            }
            manifest["status"] = "success" if not failures else "partial_success"
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

            prov = {
                **manifest,
                "duration_seconds": round(time.time() - start, 3),
                "status": status,
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
