from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd



@dataclass(frozen=True)
class FileRow:
    file_id: str
    name: str
    group: str
    release: str
    release_date: str
    directory_id: str
    snapshot_root: str
    path_rel: str
    ext: str
    size_bytes: int
    loc_total: int
    loc_blank: int
    is_py: bool
    is_test: bool
    is_docs: bool
    is_vendor: bool
    is_binary: bool
    encoding_error: bool


def load_file_index(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    required = [
        "file_id", "name", "group", "release", "release_date", "directory_id",
        "snapshot_root", "path_rel", "ext",
        "size_bytes", "loc_total", "loc_blank",
        "is_py", "is_test", "is_docs", "is_vendor", "is_binary", "encoding_error",
        "subset_core", "subset_core_plus_tests", "subset_tests_only", "subset_all_py",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"file_index is missing required columns: {missing}")
    return df


def row_to_file_row(r) -> FileRow:
    return FileRow(
        file_id=str(r["file_id"]),
        name=str(r["name"]),
        group=str(r["group"]),
        release=str(r["release"]),
        release_date=str(r["release_date"]),
        directory_id=str(r["directory_id"]),
        snapshot_root=str(r["snapshot_root"]),
        path_rel=str(r["path_rel"]),
        ext=str(r["ext"]),
        size_bytes=int(r["size_bytes"]),
        loc_total=int(r["loc_total"]),
        loc_blank=int(r["loc_blank"]),
        is_py=bool(r["is_py"]),
        is_test=bool(r["is_test"]),
        is_docs=bool(r["is_docs"]),
        is_vendor=bool(r["is_vendor"]),
        is_binary=bool(r["is_binary"]),
        encoding_error=bool(r["encoding_error"]),
    )


def read_text_file(snapshot_root: str, path_rel: str) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (text, error_reason). If error_reason is not None, text is None.
    """
    p = Path(snapshot_root) / path_rel
    try:
        # Try strict utf-8 first
        text = p.read_text(encoding="utf-8")
        return text, None
    except UnicodeDecodeError:
        # Fallback: replacement to avoid hard crashes; record this fact later if needed.
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
            return text, "unicode_decode_replaced"
        except Exception as e:
            return None, f"read_failed:{type(e).__name__}"
    except FileNotFoundError:
        return None, "file_not_found"
    except Exception as e:
        return None, f"read_failed:{type(e).__name__}"