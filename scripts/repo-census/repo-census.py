'''
Script to get important information about the various repositories.

Inputs:
    data/metadata/snapshots.csv
Outputs: 
    data/processed/file-inventory.parquet
    data/processed/repo-census.csv

USAGE:
    # Process all repos (could be heavy)
    python scripts/repo-census/repo-census.py --all

    # Process only one repo (exact name match)
    python scripts/repo-census/repo-census.py --only <repo_name>
'''
# TODO: make already run repo skip
# TODO: jsonl log doesnt exit?
# TODO: parquet is dumb change to csv?
# TODO: do we compute loc for py files in tests or or loc for everything in tests? cause I guess we only need py loc in tests
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from _lib.io import (
    SnapshotRow,
    ensure_dir,
    load_snapshots_csv,
    log_jsonl,
    resolve_snapshot_root,
    manifest_status_is_success,
)
from _lib.loc import compute_loc
from _lib.walk import (
    classify_path,
    is_python_file,
    iter_files,
)


def build_file_inventory(
    raw_root: Path,
    snapshots: list[SnapshotRow],
    log_path: Path,
) -> pd.DataFrame:
    """
    Return a huge dataframe (data/processed/inventory-files.parquet) with one row per file across all repo snapshots.
    """
    records: list[dict] = []

    run_ts = datetime.now(timezone.utc).isoformat()

    for row in snapshots:
        snap_root = resolve_snapshot_root(raw_root, row)
        if snap_root is None:
            log_jsonl(
                log_path,
                {
                    "ts": run_ts,
                    "level": "error",
                    "event": "snapshot_root_missing",
                    "repo": row.name,
                    "snapshot_id": row.snapshot_id,
                    "directory_id": row.directory_id,
                    "release": row.release,
                },
            )
            continue

        for abs_path in iter_files(snap_root):
            try:
                rel_path = abs_path.relative_to(snap_root)
            except ValueError:
                rel_path = Path(abs_path.name)

            flags = classify_path(rel_path)
            py = is_python_file(abs_path)

            loc = compute_loc(abs_path)

            try:
                size_bytes = abs_path.stat().st_size
            except OSError:
                size_bytes = None
                log_jsonl(
                    log_path,
                    {
                        "ts": run_ts,
                        "level": "warn",
                        "event": "stat_failed",
                        "repo": row.name,
                        "path": str(rel_path),
                    },
                )

            records.append(
                {
                    # snapshot identity
                    "name": row.name,
                    "group": row.group,
                    "origin_url": row.origin_url,
                    "release": row.release,
                    "release_date": row.release_date,
                    "tag_ref": row.tag_ref,
                    "snapshot_id": row.snapshot_id,
                    "revision_id": row.revision_id,
                    "directory_id": row.directory_id,
                    "snapshot_root": str(snap_root),

                    # file identity
                    "path_rel": str(rel_path),
                    "ext": abs_path.suffix.lower(),
                    "size_bytes": size_bytes,
                    "is_py": py,

                    # path class flags
                    "is_test": flags.is_test,
                    "is_docs": flags.is_docs,
                    "is_vendor": flags.is_vendor,

                    # loc stats
                    "is_binary": loc.is_binary,
                    "encoding_error": loc.encoding_error,
                    "loc_total": loc.loc_total,
                    "loc_blank": loc.loc_blank,
                }
            )

    df = pd.DataFrame.from_records(records)

    if not df.empty:
        for col in ["size_bytes", "loc_total", "loc_blank"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        for col in ["is_py", "is_test", "is_docs", "is_vendor", "is_binary", "encoding_error"]:
            df[col] = df[col].fillna(False).astype(bool)

    return df


def summarize_repo_census(file_inventory: pd.DataFrame, snapshots: list[SnapshotRow]) -> pd.DataFrame:
    """
    One row per snapshot.
    Produces total files, py files, LOC totals, and category splits.
    """
    if file_inventory.empty:
        return pd.DataFrame(
            columns=[
                "name", "group", "snapshot_id", "release", "release_date",
                "n_files_total", "n_files_py",
                "loc_total_all_text", "loc_total_py",
                "n_files_py_tests", "loc_py_tests",
                "n_files_py_vendor", "loc_py_vendor",
                "n_files_encoding_error",
                "n_files_binary",
            ]
        )

    inv = file_inventory.copy()
    inv["is_text"] = ~inv["is_binary"]

    keys = ["name", "group", "snapshot_id", "release", "release_date"]

    def n_where(mask: pd.Series) -> int:
        return int(mask.sum())

    # Group aggregate
    grouped = inv.groupby(keys, dropna=False)

    rows: list[dict] = []
    for k, g in grouped:
        g = g.copy()

        # Basic counts
        n_files_total = len(g)
        n_files_py = n_where(g["is_py"])

        # Text/binary/encoding
        n_files_binary = n_where(g["is_binary"])
        n_files_encoding_error = n_where(g["encoding_error"])

        # LOC totals (only for text files; binaries have 0 anyway)
        loc_total_all_text = int(g.loc[g["is_text"], "loc_total"].fillna(0).sum())
        loc_total_py = int(g.loc[g["is_py"] & g["is_text"], "loc_total"].fillna(0).sum())

        # Category splits (py only)
        n_files_py_tests = n_where(g["is_py"] & g["is_test"])
        loc_py_tests = int(g.loc[g["is_py"] & g["is_test"] & g["is_text"], "loc_total"].fillna(0).sum())

        n_files_py_vendor = n_where(g["is_py"] & g["is_vendor"])
        loc_py_vendor = int(g.loc[g["is_py"] & g["is_vendor"] & g["is_text"], "loc_total"].fillna(0).sum())

        rows.append(
            {
                "name": k[0],
                "group": k[1],
                "snapshot_id": k[2],
                "release": k[3],
                "release_date": k[4],
                "n_files_total": n_files_total,
                "n_files_py": n_files_py,
                "loc_total_all_text": loc_total_all_text,
                "loc_total_py": loc_total_py,
                "n_files_py_tests": n_files_py_tests,
                "loc_py_tests": loc_py_tests,
                "n_files_py_vendor": n_files_py_vendor,
                "loc_py_vendor": loc_py_vendor,
                "n_files_encoding_error": n_files_encoding_error,
                "n_files_binary": n_files_binary,
            }
        )

    census = pd.DataFrame(rows).sort_values(["group", "name"]).reset_index(drop=True)

    int_cols = [
        "n_files_total",
        "n_files_py",
        "loc_total_all_text",
        "loc_total_py",
        "n_files_py_tests",
        "loc_py_tests",
        "n_files_py_vendor",
        "loc_py_vendor",
        "n_files_encoding_error",
        "n_files_binary",
    ]
    for c in int_cols:
        census[c] = pd.to_numeric(census[c], errors="coerce").fillna(0).astype(int)

    return census


def main() -> None:
    ap = argparse.ArgumentParser(description="Compute baseline repository census metrics from materialized snapshots.")
    # Defaults are probably always gonna be correct, so don't use these!!
    ap.add_argument("--snapshots-csv", default="data/metadata/snapshots.csv", help="Path to snapshots.csv")
    ap.add_argument("--raw-root", default="data/raw/software_heritage", help="Root of materialized SWH trees")
    ap.add_argument("--out-inventory", default="data/processed/file-inventory.parquet", help="Output parquet (per-file)")
    ap.add_argument("--out-census", default="data/processed/repo-census.csv", help="Output csv (per-snapshot summary)")
    ap.add_argument("--log", default="logs/repo-census.jsonl", help="JSONL log path")
    
    group = ap.add_mutually_exclusive_group(required=True)

    # ONLY RUN SNAPSHOT WITH name
    group.add_argument("--only", help="Process only this repository name (exact match)")

    # process all repos
    group.add_argument("--all", action="store_true", help="Process only all repositories in snapshots.csv")
    args = ap.parse_args()

    snapshots_csv = Path(args.snapshots_csv)
    raw_root = Path(args.raw_root)
    out_inventory = Path(args.out_inventory)
    out_census = Path(args.out_census)
    log_path = Path(args.log)

    ensure_dir(out_inventory.parent)
    ensure_dir(out_census.parent)
    ensure_dir(log_path.parent)

    # print all snapshots loaded, before filtering
    snapshots_all = load_snapshots_csv(snapshots_csv)
    print(f"Snapshots loaded:   {len(snapshots_all)}")
    snapshots = snapshots_all

    snapshots = [
        row
        for row in snapshots
        if (root := resolve_snapshot_root(raw_root, row)) is not None
        and manifest_status_is_success(root)
    ]
    # print snapshots with successful _MANIFEST.json
    print(f"Snapshots success:  {len(snapshots)}")

    if args.only:
        # print only repo to print
        print(f"Filtering to repo:  {args.only}")
        snapshots = [row for row in snapshots if row.name == args.only]
        print(f"Snapshots selected: {len(snapshots)}")

    inventory = build_file_inventory(
        raw_root=raw_root,
        snapshots=snapshots,
        log_path=log_path,
    )

    # Write inventory parquet
    inventory.to_parquet(out_inventory, index=False)

    # Write census CSV
    census = summarize_repo_census(inventory, snapshots)
    census.to_csv(out_census, index=False)

    print(f"Wrote inventory: {out_inventory} ({len(inventory):,} files)")
    print(f"Wrote census:    {out_census} ({len(census):,} snapshots)")
    print(f"Log:            {log_path}")


if __name__ == "__main__":
    main()