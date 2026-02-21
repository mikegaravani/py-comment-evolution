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

import dotenv
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
import tarfile
from urllib.parse import urljoin

import requests

SWH_API = "https://archive.softwareheritage.org/api/1"

SNAPSHOTS_CSV = Path("data/metadata/snapshots.csv")
OUT_ROOT = Path("data/raw/software_heritage")
PROVENANCE_JSONL = Path("data/metadata/provenance.jsonl")

SESSION = requests.Session()
dotenv.load_dotenv()
token = os.getenv("SWH_TOKEN")
SESSION.headers.update(
    {
        "User-Agent": "thesis-swh-fetch",
        "Accept": "application/json",
    }
)
if token:
    SESSION.headers["Authorization"] = f"Bearer {token}"

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


def swh_dir_swhid(directory_id: str) -> str:
    # directory_id in your CSV is already the sha1_git hex (no prefix)
    return f"swh:1:dir:{directory_id}"


def http_post_json(url: str, data: Optional[dict] = None, retries: int = 10) -> Any:
    last_err: Optional[Exception] = None
    last_status: Optional[int] = None
    last_text: Optional[str] = None

    for attempt in range(retries):
        try:
            r = SESSION.post(url, data=data, timeout=DEFAULT_TIMEOUT)
            last_status = r.status_code

            if r.status_code == 429:
                reset = r.headers.get("X-Ratelimit-Reset")
                remaining = r.headers.get("X-Ratelimit-Remaining")
                limit = r.headers.get("X-Ratelimit-Limit")

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
                    f"  RETRY {attempt+1}/{retries} 429 POST {url} "
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
            return r.json()

        except Exception as e:
            last_err = e
            sleep_s = min(15.0, 1.25 ** attempt) + random.random() * 0.25
            print(f"  RETRY {attempt+1}/{retries} EXC {type(e).__name__} POST {url} -> sleeping {sleep_s:.2f}s")
            time.sleep(sleep_s)

    detail = []
    if last_status is not None:
        detail.append(f"last_status={last_status}")
    if last_text:
        detail.append(f"last_body={last_text!r}")
    if last_err is not None:
        detail.append(f"last_err={last_err!r}")
    suffix = ("; " + ", ".join(detail)) if detail else ""
    raise FetchError(f"POST failed after {retries} retries: {url}{suffix}")


def vault_flat_cook_status(swhid: str) -> Dict[str, Any]:
    # GET /api/1/vault/flat/<swhid>/
    url = f"{SWH_API}/vault/flat/{swhid}/"
    data = http_get_json(url)
    if not isinstance(data, dict):
        raise FetchError(f"Unexpected vault status payload for {swhid}: {type(data)}")
    return data


def vault_flat_request_cook(swhid: str) -> Dict[str, Any]:
    # POST /api/1/vault/flat/<swhid>/
    url = f"{SWH_API}/vault/flat/{swhid}/"
    data = http_post_json(url)
    if not isinstance(data, dict):
        raise FetchError(f"Unexpected vault cook payload for {swhid}: {type(data)}")
    return data


def vault_flat_download_url(status_payload: Dict[str, Any]) -> str:
    fetch_url = status_payload.get("fetch_url")
    if not isinstance(fetch_url, str) or not fetch_url:
        raise FetchError(f"Missing fetch_url in vault payload: keys={list(status_payload.keys())}")

    if fetch_url.startswith("http://") or fetch_url.startswith("https://"):
        return fetch_url
    return urljoin(SWH_API + "/", fetch_url.lstrip("/"))


def log(msg: str) -> None:
    print(f"[{utc_now_iso()}] {msg}", flush=True)


def vault_flat_cook_and_download(
    directory_id: str,
    out_dir: Path,
    *,
    poll_interval_s: float = 3.0,
    max_poll_s: float = 60 * 60,
) -> Tuple[Path, Dict[str, Any]]:
    """
    Cook + download + extract a flat tarball for directory_id into out_dir.

    Returns (tarball_path, final_status_payload)
    """
    swhid = swh_dir_swhid(directory_id)

    log(f"vault: request cook {swhid}")
    cook_payload = vault_flat_request_cook(swhid)

    start = time.time()
    last = cook_payload
    i = 0
    last_msg = None

    log(f"vault: polling {swhid}")
    while True:
        status = last.get("status")
        msg = last.get("progress_message") or last.get("message") or ""

        if status in ("done", "failed"):
            log(f"vault: terminal status={status} {msg}".strip())
            break

        elapsed = int(time.time() - start)
        if elapsed > max_poll_s:
            raise FetchError(
                f"Vault cook timed out after {int(max_poll_s)}s for {swhid} "
                f"(last_status={status}, msg={msg})"
            )

        # log every 10 polls OR if progress message changes
        if i % 10 == 0 or (msg and msg != last_msg):
            log(f"vault: status={status} elapsed={elapsed}s {msg}".strip())
            last_msg = msg

        # adaptive backoff: reduces API calls, avoids rate limits
        sleep_s = min(30.0, poll_interval_s * (1.15 ** (i // 20))) + random.random() * 0.5
        time.sleep(sleep_s)
        last = vault_flat_cook_status(swhid)
        i += 1

    if last.get("status") == "failed":
        raise FetchError(f"Vault cook failed for {swhid}: {last}")

    dl_url = vault_flat_download_url(last)
    tarball_path = out_dir / "_SWH_VAULT_FLAT.tar.gz"

    log(f"vault: downloading {dl_url}")
    ensure_parent(tarball_path)

    with SESSION.get(dl_url, timeout=DEFAULT_TIMEOUT, stream=True) as r:
        if r.status_code == 429:
            # fall back to your existing retry logic (non-stream)
            log("vault: download hit 429, falling back to retrying non-stream download")
            r2 = http_get(dl_url)
            tarball_path.write_bytes(r2.content)
        else:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", "0") or 0)
            written = 0
            t0 = time.time()

            with tarball_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    written += len(chunk)

                    if total:
                        if written // (50 * 1024 * 1024) != (written - len(chunk)) // (50 * 1024 * 1024):
                            dt = max(0.001, time.time() - t0)
                            mbps = (written / (1024 * 1024)) / dt
                            log(f"download: {written/1024/1024:.0f}MB / {total/1024/1024:.0f}MB ({mbps:.1f} MB/s)")
                    else:
                        if written // (10 * 1024 * 1024) != (written - len(chunk)) // (10 * 1024 * 1024):
                            log(f"download: {written/1024/1024:.0f}MB")

    with tarfile.open(tarball_path, "r:gz") as tf:
        # ensure no path traversal inside tar
        for member in tf.getmembers():
            member_path = (out_dir / member.name).resolve()
            if not str(member_path).startswith(str(out_dir.resolve())):
                raise FetchError(f"Unsafe path in tar: {member.name}")
        log(f"vault: extracting {tarball_path}")
        tf.extractall(out_dir)
        log("vault: extraction complete")

    return tarball_path, last


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
            tarball_path, vault_status = vault_flat_cook_and_download(row.directory_id, out_dir)

            # optional: build a rel_paths list by walking the extracted directory
            rel_paths: List[str] = []
            for p in out_dir.rglob("*"):
                if p.is_file() and p.name not in ("_MANIFEST.json", "_SWH_VAULT_FLAT.tar.gz"):
                    rel_paths.append(safe_relpath(os.path.relpath(p, out_dir)))

            manifest = {
                "repo": row.name,
                "release": row.release,
                "origin_url": row.origin_url,
                "tag_ref": row.tag_ref,
                "snapshot_id": row.snapshot_id,
                "revision_id": row.revision_id,
                "directory_id": row.directory_id,
                "directory_swhid": swh_dir_swhid(row.directory_id),
                "retrieved_at": utc_now_iso(),
                "vault": {
                    "type": "flat",
                    "status_payload": vault_status,
                    "tarball": tarball_path.name,
                    "tarball_sha256": sha256_file(tarball_path),
                },
                "file_count": len(rel_paths),
                "manifest_paths_sha256": compute_manifest_hash(rel_paths),
                "directory_failures": [],
                "status": "success",
            }
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

            prov = {
                **manifest,
                "duration_seconds": round(time.time() - start, 3),
                "status": "success",
            }
            write_provenance(prov)

            print(f"  OK: extracted_files={len(rel_paths)}")
            print(f"  tarball: {tarball_path}")
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
