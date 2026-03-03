"""
Build a unified file index (single source of truth) from per-repo inventory CSVs.

Inputs:
  data/processed/inventory/*.csv   (all the per-repo invetory csvs)

Outputs:
  data/processed/file_index/file_index.parquet
  data/processed/file_index/file_index.csv (for debugging)
  data/processed/file_index/file_index_summary_by_repo.csv
  data/processed/file_index/file_index_summary_by_group.csv

Notes:
- This script does NOT walk the filesystem, it trusts inventory CSVs produced earlier.
- Subsets are encoded as boolean flags in the canonical index.

USAGE:
    python scripts/comment-extraction/build-file-index.py (for csv: --also-write-csv)
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import os
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_INVENTORY_GLOB = "data/processed/inventory/*-file-inventory.csv"
DEFAULT_OUT_PARQUET = "data/processed/file_index/file_index.parquet"


REQUIRED_COLUMNS = [
    "name",
    "group",
    "origin_url",
    "release",
    "release_date",
    "directory_id",
    "snapshot_root",
    "path_rel",
    "ext",
    "is_py",
    "is_test",
    "is_docs",
    "is_vendor",
    "is_binary",
    "encoding_error",
    "size_bytes",
    "loc_total",
    "loc_blank",
]


def stable_file_id(repo: str, release: str, path_rel: str) -> str:
    """Stable ID for joins across pipeline stages (redundant but nice to have)"""
    s = f"{repo}::{release}::{path_rel}".encode("utf-8", errors="replace")
    return hashlib.sha1(s).hexdigest()


def read_inventory_csvs(paths: Iterable[str]) -> pd.DataFrame:
    dfs = []
    for p in paths:
        df = pd.read_csv(
            p,
            dtype={
                "name": "string",
                "group": "string",
                "origin_url": "string",
                "release": "string",
                "release_date": "string",
                "directory_id": "string",
                "snapshot_root": "string",
                "path_rel": "string",
                "ext": "string",
            },)
        df["__inventory_source"] = os.path.basename(p)
        dfs.append(df)

    if not dfs:
        raise FileNotFoundError("No inventory CSVs found. Check your inventory glob/path.")

    out = pd.concat(dfs, ignore_index=True)

    out.columns = [c.strip() for c in out.columns]
    return out


def ensure_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in inventory data: {missing}\n"
            "Either update REQUIRED_COLUMNS or fix the inventory generation step!!"
        )


def coerce_bool_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    # Makes invetory robust in case instead of a bool we have "True"/"False"/1/0.
    def to_bool(x):
        if pd.isna(x):
            return False
        if isinstance(x, bool):
            return x
        if isinstance(x, (int, float)):
            return bool(int(x))
        s = str(x).strip().lower()
        return s in {"true", "1", "yes", "y", "t"}

    for c in cols:
        df[c] = df[c].map(to_bool)
    return df


def build_subset_flags(df: pd.DataFrame) -> pd.DataFrame:
    valid_text_py = df["is_py"] & (~df["is_binary"]) & (~df["encoding_error"])
    no_vendor_docs = (~df["is_vendor"]) & (~df["is_docs"])

    df["subset_core"] = valid_text_py & no_vendor_docs & (~df["is_test"]) # only core prod code
    df["subset_core_plus_tests"] = valid_text_py & no_vendor_docs # core code + tests
    df["subset_tests_only"] = valid_text_py & no_vendor_docs & (df["is_test"]) # only tests
    df["subset_all_py"] = valid_text_py # all python files

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Build file_index.parquet from inventory CSVs.")
    # do not use these two args
    parser.add_argument("--inventory-glob", default=DEFAULT_INVENTORY_GLOB, help="Glob for inventory CSVs.")
    parser.add_argument("--out-parquet", default=DEFAULT_OUT_PARQUET, help="Output parquet path.")

    parser.add_argument(
        "--also-write-csv",
        action="store_true",
        help="Also write data/processed/file_index/file_index.csv for debugging.",
    )
    args = parser.parse_args()

    inv_paths = sorted(glob.glob(args.inventory_glob))
    if not inv_paths:
        raise FileNotFoundError(f"No inventory files matched: {args.inventory_glob}")

    df = read_inventory_csvs(inv_paths)
    ensure_required_columns(df)

    # Coerce types
    bool_cols = ["is_py", "is_test", "is_docs", "is_vendor", "is_binary", "encoding_error"]
    df = coerce_bool_columns(df, bool_cols)

    # Ensure numeric cols are numeric
    for c in ["size_bytes", "loc_total", "loc_blank"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    # Add file_id
    df["file_id"] = [
        stable_file_id(repo=str(r), release=str(rel), path_rel=str(p))
        for r, rel, p in zip(df["name"], df["release"], df["path_rel"])
    ]

    # Add subset flags
    df = build_subset_flags(df)

    # Reorder columns for readability
    preferred_order = [
        "file_id",
        "name",
        "group",
        "origin_url",
        "release",
        "release_date",
        "directory_id",
        "snapshot_root",
        "path_rel",
        "ext",
        "size_bytes",
        "loc_total",
        "loc_blank",
        "is_py",
        "is_test",
        "is_docs",
        "is_vendor",
        "is_binary",
        "encoding_error",
        "subset_core",
        "subset_core_plus_tests",
        "subset_tests_only",
        "subset_all_py",
        "__inventory_source",
    ]
    extra_cols = [c for c in df.columns if c not in preferred_order]
    df = df[preferred_order + extra_cols]

    # Ensure output directory exists
    out_parquet = Path(args.out_parquet)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    # Write parquet
    df.to_parquet(out_parquet, index=False)

    if args.also_write_csv:
        out_csv = out_parquet.with_suffix(".csv")
        df.to_csv(out_csv, index=False)

    summary_by_repo = (
        df.groupby(["name", "group", "release"], as_index=False)
        .agg(
            n_files=("file_id", "count"),
            n_py=("is_py", "sum"),
            loc_py=("loc_total", lambda s: int(s[df.loc[s.index, "is_py"]].sum())),
            n_core=("subset_core", "sum"),
            loc_core=("loc_total", lambda s: int(s[df.loc[s.index, "subset_core"]].sum())),
            n_core_plus_tests=("subset_core_plus_tests", "sum"),
            loc_core_plus_tests=("loc_total", lambda s: int(s[df.loc[s.index, "subset_core_plus_tests"]].sum())),
            n_tests_only=("subset_tests_only", "sum"),
            loc_tests_only=("loc_total", lambda s: int(s[df.loc[s.index, "subset_tests_only"]].sum())),
            n_all_py=("subset_all_py", "sum"),
            loc_all_py=("loc_total", lambda s: int(s[df.loc[s.index, "subset_all_py"]].sum())),
        )
    )

    summary_by_group = (
        df.groupby(["group"], as_index=False)
        .agg(
            n_files=("file_id", "count"),
            n_py=("is_py", "sum"),
            n_core=("subset_core", "sum"),
            n_core_plus_tests=("subset_core_plus_tests", "sum"),
            n_tests_only=("subset_tests_only", "sum"),
            n_all_py=("subset_all_py", "sum"),
            loc_total=("loc_total", "sum"),
            loc_all_py=("loc_total", lambda s: int(s[df.loc[s.index, "subset_all_py"]].sum())),
            loc_core=("loc_total", lambda s: int(s[df.loc[s.index, "subset_core"]].sum())),
            loc_tests_only=("loc_total", lambda s: int(s[df.loc[s.index, "subset_tests_only"]].sum())),
        )
    )

    summary_repo_path = out_parquet.parent / "file_index_summary_by_repo.csv"
    summary_group_path = out_parquet.parent / "file_index_summary_by_group.csv"

    summary_by_repo.to_csv(summary_repo_path, index=False)
    summary_by_group.to_csv(summary_group_path, index=False)

    print(f"Wrote: {out_parquet}")
    if args.also_write_csv:
        print(f"Wrote: {out_parquet.with_suffix('.csv')}")
    print(f"Wrote: {summary_repo_path}")
    print(f"Wrote: {summary_group_path}")

    print("\nTop-level sanity check (per repo):")
    cols = [
        "name",
        "group",
        "release",
        "n_core",
        "n_core_plus_tests",
        "n_tests_only",
        "n_all_py",
        "loc_core",
        "loc_tests_only",
    ]
    print(summary_by_repo[cols].sort_values(["group", "name"]).to_string(index=False))


if __name__ == "__main__":
    main()