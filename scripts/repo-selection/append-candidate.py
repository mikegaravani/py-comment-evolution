from __future__ import annotations

from dotenv import load_dotenv
from datetime import datetime, timezone
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests

# Appends a candidate repository to data/metadata/candidates.json, with useful repo metadata.
# USAGE: python scripts/repo-selection/append-candidate.py <owner/repo> [eventually more <owner/repo> ...]

load_dotenv()
GITHUB_API = "https://api.github.com"
CANDIDATES_PATH = Path("data/metadata/candidates.json")

PROTECTED_FIELDS: Dict[str, Any] = {
    "notes": "",
    "repo_time_period": "2020s",
    "is_chosen_candidate": "",
}

def _headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "candidates-updater/1.0",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _parse_owner_repo(repo_full: str) -> Tuple[str, str]:
    repo_full = repo_full.strip().rstrip("/")
    if repo_full.count("/") != 1:
        raise ValueError(f"Repository must look like 'owner/repo', got: {repo_full!r}")
    owner, repo = repo_full.split("/", 1)
    if not owner or not repo:
        raise ValueError(f"Invalid repository: {repo_full!r}")
    return owner, repo


def _get_json(url: str, params: Optional[dict] = None) -> Tuple[dict, requests.Response]:
    resp = requests.get(url, headers=_headers(), params=params, timeout=30)
    if resp.status_code >= 400:
        try:
            detail = resp.json()
        except Exception:
            detail = {"message": resp.text}
        msg = detail.get("message", "Unknown error")
        raise RuntimeError(f"GitHub API error {resp.status_code} for {url}: {msg}")
    return resp.json(), resp


def _count_via_link_header(url: str, params: dict) -> int:
    params = dict(params)
    params["per_page"] = 1

    data, resp = _get_json(url, params=params)

    link = resp.headers.get("Link", "")
    if not link:
        return len(data) if isinstance(data, list) else 0

    last_url = None
    for part in link.split(","):
        part = part.strip()
        if 'rel="last"' in part:
            start = part.find("<") + 1
            end = part.find(">")
            if start > 0 and end > start:
                last_url = part[start:end]
            break

    if not last_url:
        return len(data) if isinstance(data, list) else 0

    from urllib.parse import urlparse, parse_qs

    qs = parse_qs(urlparse(last_url).query)
    page_vals = qs.get("page", [])
    if not page_vals:
        return len(data) if isinstance(data, list) else 0
    return int(page_vals[0])

def _get_main_language(owner: str, repo: str) -> Tuple[str, float, Dict[str, int]]:
    url = f"{GITHUB_API}/repos/{owner}/{repo}/languages"
    langs, _ = _get_json(url)

    if not isinstance(langs, dict) or not langs:
        return "", 0.0, {}

    total = sum(int(v) for v in langs.values())
    if total <= 0:
        return "", 0.0, {k: int(v) for k, v in langs.items()}

    main_lang, main_bytes = max(langs.items(), key=lambda kv: int(kv[1]))
    pct = (int(main_bytes) / total) * 100.0
    return str(main_lang), round(pct, 1), {k: int(v) for k, v in langs.items()}



def _load_candidates(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, list):
        raise ValueError(f"{path} must contain a JSON list/array.")
    return obj


def _save_candidates(path: Path, candidates: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(candidates, f, indent=2, ensure_ascii=False)
        f.write("\n")


def build_entry(repo_full: str) -> Dict[str, Any]:
    owner, repo = _parse_owner_repo(repo_full)

    data_collection_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    repo_url = f"{GITHUB_API}/repos/{owner}/{repo}"
    repo_data, _ = _get_json(repo_url)

    main_language, main_language_pct, languages_bytes = _get_main_language(owner, repo)

    html_url = repo_data.get("html_url") or f"https://github.com/{owner}/{repo}"
    created_at = repo_data.get("created_at", "")

    stars = int(repo_data.get("stargazers_count", 0))

    default_branch = repo_data.get("default_branch", "main")
    commits_url = f"{GITHUB_API}/repos/{owner}/{repo}/commits"
    commits_count = _count_via_link_header(commits_url, params={"sha": default_branch})

    releases_url = f"{GITHUB_API}/repos/{owner}/{repo}/releases"
    releases_count = _count_via_link_header(releases_url, params={})

    contributors_url = f"{GITHUB_API}/repos/{owner}/{repo}/contributors"
    contributors_count = _count_via_link_header(contributors_url, params={"anon": "true"})

    return {
        "name": repo_full,
        "url": html_url,
        "repo_creation_date": created_at,
        "data_collection_timestamp": data_collection_timestamp,
        "commits_number": commits_count,
        "stars_number": stars,
        "releases_number": releases_count,
        "contributors_number": contributors_count,
        "main_language": main_language,
        "main_language_percentage": main_language_pct,
        "languages_bytes": languages_bytes,
    }


def upsert_candidate(path: Path, entry: Dict[str, Any]) -> None:
    candidates = _load_candidates(path)

    name = entry.get("name")
    idx = next((i for i, c in enumerate(candidates) if c.get("name") == name), None)

    if idx is None:
        for field, default in PROTECTED_FIELDS.items():
            entry.setdefault(field, default)

        candidates.append(entry)
        action = "Appended"
    else:
        old = candidates[idx]
        merged = {**old, **entry}

        for field, default in PROTECTED_FIELDS.items():
            if field in old:
                merged[field] = old[field]
            else:
                merged[field] = default

        candidates[idx] = merged
        action = "Updated"

    _save_candidates(path, candidates)
    print(f"{action} entry for {name} in {path}")


def main(argv: List[str]) -> int:
    if len(argv) < 2 or argv[1] in {"-h", "--help"}:
        print(__doc__.strip())
        return 0

    repo_fulls = argv[1:]

    any_failed = False
    for repo_full in repo_fulls:
        try:
            entry = build_entry(repo_full)
            upsert_candidate(CANDIDATES_PATH, entry)
        except Exception as e:
            any_failed = True
            print(f"ERROR processing {repo_full!r}: {e}", file=sys.stderr)

    return 1 if any_failed else 0



if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
