'''
In charge of walking through directory trees ad nd classifying files.
'''
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
import yaml


@dataclass(frozen=True)
class PathFlags:
    is_test: bool
    is_docs: bool
    is_vendor: bool


def load_exclusions(config_path: Path | None = None):
    """
    Load exclusions + classification rules from config/exclusions.yaml
    """
    if config_path is None:
        repo_root = Path(__file__).resolve().parents[3]
        config_path = repo_root / "config" / "exclusions.yaml"

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    exclude_dirs = {str(x) for x in data.get("exclude_dirs", [])}

    exclude_files = {str(x) for x in data.get("exclude_files", [])}

    classify_paths = {
        str(k): {str(v).lower() for v in values}
        for k, values in (data.get("classify_paths", {}) or {}).items()
    }

    return exclude_dirs, exclude_files, classify_paths


_EXCLUDE_DIRS, _EXCLUDE_FILES, _CLASSIFY_PATHS = load_exclusions()


def iter_files(root: Path) -> Iterator[Path]:
    """
    Deterministic recursive file iterator.
    Skips directories by *name* (any path segment equal to an excluded name).
    """
    
    stack = [root]

    while stack:
        current = stack.pop()
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.name)
        except (PermissionError, OSError):
            continue

        for p in entries:
            try:
                if p.is_symlink():
                    continue
            except OSError:
                continue

            if p.is_dir():
                if p.name in _EXCLUDE_DIRS:
                    continue
                stack.append(p)
            elif p.is_file():
                if p.name in _EXCLUDE_FILES:
                    continue
                yield p


def is_python_file(path: Path) -> bool:
    return path.suffix == ".py"


def classify_path(path_rel: Path) -> PathFlags:
    """
    Classify based on path segments.
    path_rel must be relative to snapshot root.
    """

    parts = {p.lower() for p in path_rel.parts}

    tests = any(seg in parts for seg in _CLASSIFY_PATHS.get("tests", set()))
    docs = any(seg in parts for seg in _CLASSIFY_PATHS.get("docs", set()))
    vendor = any(seg in parts for seg in _CLASSIFY_PATHS.get("vendor", set()))

    return PathFlags(is_test=tests, is_docs=docs, is_vendor=vendor)