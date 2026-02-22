"""
Default job:
Takes the origin URL and release tags from config/repos.yaml, and
produces the Software Heritage snapshot, revision, and directory IDs in data/metadata/snapshots.csv.
  - Inputs:  config/repos.yaml
  - Outputs: data/metadata/snapshots.csv
USAGE: python scripts/repo-collection/resolve-snapshots.py

Alternate job:
Takes the candidates for the 2000s repos and produces the Software Heritage snapshot, revision, and directory IDs.
  - Inputs:  data/metadata/repo-selection/initial_candidates_2000s.yaml
  - Outputs: data/metadata/repo-selection/initial_candidates_2000s_snapshots.csv
USAGE: python scripts/repo-collection/resolve-snapshots.py --job candidates_2000s
"""

from __future__ import annotations

import argparse
import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

SWH_API = "https://archive.softwareheritage.org/api/1"

DEFAULT_CONFIG_PATH = Path("config/repos.yaml")
DEFAULT_OUT_CSV = Path("data/metadata/snapshots.csv")

CANDIDATES_2000S_PATH = Path("data/metadata/repo-selection/initial_candidates_2000s.yaml")
CANDIDATES_2000S_OUT_CSV = Path(
    "data/metadata/repo-selection/initial_candidates_2000s_snapshots.csv"
)

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "thesis-swh-resolver",
        "Accept": "application/json",
    }
)

DEFAULT_TIMEOUT = 30


@dataclass
class RepoConfig:
    name: str
    url: str
    group: str
    release: str

@dataclass
class JobSpec:
    input_yaml: Path
    output_csv: Path

class ResolutionError(RuntimeError):
    pass


def http_get_json(url: str, params: Optional[dict] = None, retries: int = 5) -> Any:
    last_err = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            if r.status_code == 429:
                # basic backoff for rate limiting
                sleep_s = 1.5 * (attempt + 1)
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.8 * (attempt + 1))
    raise ResolutionError(f"GET failed after {retries} retries: {url} ({last_err})")


def load_repos_from_yaml(path: Path) -> List[RepoConfig]:
    if not path.exists():
        raise ResolutionError(f"Missing config file: {path}")
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict) or "repos" not in cfg:
        raise ResolutionError("Config must be a dict with top-level key: repos")

    repos: List[RepoConfig] = []
    for item in cfg["repos"]:
        repos.append(
            RepoConfig(
                name=str(item["name"]).strip(),
                url=str(item["url"]).strip(),
                group=str(item["group"]).strip(),
                release=str(item["release"]).strip(),
            )
        )
    return repos


def origin_get(origin_url: str) -> dict:
    # GET /api/1/origin/<origin_url>/get/
    url = f"{SWH_API}/origin/{origin_url}/get/"
    return http_get_json(url)


def latest_visit_with_snapshot(origin_url: str) -> dict:
    # Prefer: GET /api/1/origin/<origin_url>/visit/latest/?require_snapshot=true
    url = f"{SWH_API}/origin/{origin_url}/visit/latest/"
    try:
        return http_get_json(url, params={"require_snapshot": "true"})
    except ResolutionError:
        # Fallback: GET /api/1/origin/<origin_url>/visits/
        visits_url = f"{SWH_API}/origin/{origin_url}/visits/"
        visits = http_get_json(visits_url)
        if not isinstance(visits, list) or not visits:
            raise ResolutionError("No visits found for origin")
        for v in visits:
            if v.get("status") == "full" and v.get("snapshot"):
                return v
        raise ResolutionError("No FULL visit with snapshot found for origin")


def snapshot_fetch_branch(snapshot_id: str, branch_name: str) -> Optional[dict]:
    """
    Try to retrieve a specific branch from the snapshot, without loading all branches.
    Uses snapshot query parameters:
      - branches_from
      - branches_count
    """
    url = f"{SWH_API}/snapshot/{snapshot_id}/"
    # Ask for a small window starting at branch_name
    data = http_get_json(
        url,
        params={
            "branches_from": branch_name,
            "branches_count": 50,
        },
    )
    branches = data.get("branches") or {}
    return branches.get(branch_name)


def snapshot_get_alias_target(snapshot_id: str, alias_branch: str, max_hops: int = 10) -> dict:
    """
    Resolve alias branches within the same snapshot (e.g., HEAD -> refs/heads/main).
    """
    seen = set()
    current = alias_branch
    for _ in range(max_hops):
        if current in seen:
            raise ResolutionError(f"Alias loop detected while resolving {alias_branch}")
        seen.add(current)
        b = snapshot_fetch_branch(snapshot_id, current)
        if not b:
            raise ResolutionError(f"Alias branch not found in snapshot: {current}")
        if b.get("target_type") != "alias":
            return b
        current = b.get("target")
        if not isinstance(current, str):
            raise ResolutionError(f"Invalid alias target for {alias_branch}")
    raise ResolutionError(f"Alias resolution exceeded {max_hops} hops for {alias_branch}")


def tag_candidates(repo_name: str, release: str) -> List[str]:
    """
    Try to find the correct tag name and prefix for the release.
    """
    r = release.strip()
    n = repo_name.strip()
    bases = [
        r,
        f"v{r}",
        f"{n}-{r}",
        f"{n}_{r}",
        f"{n}{r}",
        f"{n}-v{r}",
        f"{n}v{r}",
        f"{n}_v{r}",
    ]

    prefixes = [
        "refs/tags/",
        "releases/",
        "release/",
    ]

    suffixes = [
        f"/{n}-{r}.tar.gz",
        "",
    ]

    seen = set()
    out: List[str] = []
    out.append(f'refs/tags/v{r}')
    for base in bases:
        for pref in prefixes:
            for suff in suffixes:
                ref = f"{pref}{base}{suff}"
                if ref not in seen:
                    pass
                    # seen.add(ref)
                    # out.append(ref)
    return out


def resolve_tag_to_revision_and_directory(snapshot_id: str, tag_ref: str) -> Tuple[str, str, str, str]:
    """
    Returns (tag_ref, revision_id, directory_id, release_date)
    Release date can be "".
    """
    b = snapshot_fetch_branch(snapshot_id, tag_ref)
    if not b:
        raise ResolutionError(f"Tag ref not present in snapshot: {tag_ref}")

    if b.get("target_type") == "alias":
        b = snapshot_get_alias_target(snapshot_id, tag_ref)

    target_type = b.get("target_type")
    target = b.get("target")

    if not isinstance(target, str):
        raise ResolutionError(f"Invalid target for {tag_ref}: {target}")

    if target_type == "revision":
        rev = http_get_json(f"{SWH_API}/revision/{target}/")
        directory = rev.get("directory")
        if not isinstance(directory, str):
            raise ResolutionError(f"Revision missing directory: {target}")
        release_date = rev.get("date") or ""
        if not isinstance(release_date, str):
            release_date = ""
        return tag_ref, target, directory, release_date

    if target_type == "release":
        rel = http_get_json(f"{SWH_API}/release/{target}/")
        release_date = rel.get("date") or ""
        if not isinstance(release_date, str):
            release_date = ""

        rel_target_type = rel.get("target_type")
        rel_target = rel.get("target")
        if not isinstance(rel_target, str) or not isinstance(rel_target_type, str):
            raise ResolutionError(f"Release missing target: {target}")

        if rel_target_type == "revision":
            rev = http_get_json(f"{SWH_API}/revision/{rel_target}/")
            directory = rev.get("directory")
            if not isinstance(directory, str):
                raise ResolutionError(f"Revision missing directory: {rel_target}")
            return tag_ref, rel_target, directory, release_date

        if rel_target_type == "directory":
            return tag_ref, "", rel_target, release_date

        raise ResolutionError(
            f"Release {target} points to unsupported target_type: {rel_target_type}"
        )

    raise ResolutionError(f"Unsupported tag target_type: {target_type} for {tag_ref}")


def resolve_one(repo: RepoConfig) -> Dict[str, str]:
    # 1) origin get (validates origin exists)
    _ = origin_get(repo.url)

    # 2) pick latest full visit that includes snapshot
    visit = latest_visit_with_snapshot(repo.url)
    snapshot_id = visit.get("snapshot")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise ResolutionError("Visit did not include snapshot id")

    # 3) resolve tag by trying candidates
    last_err = None
    for ref in tag_candidates(repo.name, repo.release):
        try:
            tag_ref, revision_id, directory_id, release_date = resolve_tag_to_revision_and_directory(snapshot_id, ref)
            return {
                "name": repo.name,
                "group": repo.group,
                "origin_url": repo.url,
                "release": repo.release,
                "release_date": release_date,
                "tag_ref": tag_ref,
                "snapshot_id": snapshot_id,
                "revision_id": revision_id,
                "directory_id": directory_id,
            }
        except Exception as e:
            last_err = e
            continue

    raise ResolutionError(
        f"Could not resolve release tag '{repo.release}' for {repo.name}. "
        f"Tried candidates: {tag_candidates(repo.name, repo.release)}. Last error: {last_err}"
    )

def load_existing_resolutions(csv_path: Path) -> set[tuple[str, str]]:
    """
    Returns a set of (origin_url, release) already present in CSV.
    """
    if not csv_path.exists():
        return set()

    resolved = set()
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            origin = row.get("origin_url")
            release = row.get("release")
            if origin and release:
                resolved.add((origin.strip(), release.strip()))
    return resolved


def resolve_repos_to_csv(repos: List[RepoConfig], out_csv: Path) -> int:
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    existing = load_existing_resolutions(out_csv)
    print(f"Found {len(existing)} existing resolutions")

    rows: List[Dict[str, str]] = []
    
    for repo in repos:
        key = (repo.url, repo.release)
        if key in existing:
            print(f"Skipping (already resolved): {repo.name} release={repo.release}")
            continue
            
        print(f"Resolving: {repo.name} ({repo.url}) release={repo.release}")
        try:
            row = resolve_one(repo)
            rows.append(row)
            print(f"  OK: snapshot={row['snapshot_id']} tag_ref={row['tag_ref']}")
        except Exception as e:
            print(f"  ERROR: {e}")

    fieldnames = [
        "name",
        "group",
        "origin_url",
        "release",
        "release_date",
        "tag_ref",
        "snapshot_id",
        "revision_id",
        "directory_id",
    ]

    write_header = (not out_csv.exists()) or (out_csv.stat().st_size == 0)

    with out_csv.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow(r)

    if not rows:
        print("No new repos to resolve; nothing to write.")
        return 0

    print(f"\nUpdated: {out_csv} (+{len(rows)} rows)")
    return len(rows)


def get_job_spec(job: str) -> JobSpec:
    """
    Job presets. Default is what should be used to produce the snapshots.
    """
    if job == "default":
        return JobSpec(input_yaml=DEFAULT_CONFIG_PATH, output_csv=DEFAULT_OUT_CSV)
    if job == "candidates_2000s":
        return JobSpec(input_yaml=CANDIDATES_2000S_PATH, output_csv=CANDIDATES_2000S_OUT_CSV)
    raise ResolutionError(f"Unknown job '{job}'. Use 'default' or 'candidates_2000s'.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Resolve SWH snapshot/revision/directory IDs for repos.")
    p.add_argument(
        "--job",
        default="default",
        choices=["default", "candidates_2000s"],
        help="Which input/output preset to run (default: default).",
    )
    # EXTRA OVERRIDES (generally NOT needed)
    p.add_argument("--input", type=Path, default=None, help="Override input YAML path.")
    p.add_argument("--output", type=Path, default=None, help="Override output CSV path.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spec = get_job_spec(args.job)

    input_yaml = args.input if args.input is not None else spec.input_yaml
    output_csv = args.output if args.output is not None else spec.output_csv

    repos = load_repos_from_yaml(input_yaml)
    resolve_repos_to_csv(repos, output_csv)

if __name__ == "__main__":
    main()
