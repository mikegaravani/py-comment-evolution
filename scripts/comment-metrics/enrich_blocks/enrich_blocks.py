"""
Enrich comment blocks with features for classification.

USAGE:
    python scripts/comment-metrics/enrich_blocks/enrich_blocks.py --subset core
    python scripts/comment-metrics/enrich_blocks/enrich_blocks.py --all
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from features import get_feature_pipeline # in features/__init__.py
from io_utils import SUBSETS, read__blocks, write_enriched_blocks


def _validate_blocks_df(df: pd.DataFrame) -> None:
    required = {
        "subset",
        "block_id",
        "block_kind",
        "repo",
        "group",
        "release",
        "file_id",
        "path_rel",
        "start_lineno",
        "end_lineno",
        "indent_col",
        "n_lines",
        "block_text_stripped",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Blocks parquet missing required columns: {sorted(missing)}")

    if df["block_id"].isna().any():
        raise ValueError("Found null block_id values.")

    if df["block_id"].duplicated().any():
        dups = int(df["block_id"].duplicated().sum())
        raise ValueError(f"Found duplicated block_id values ({dups}).")


def enrich_subset(subset: str) -> Path:
    df = read__blocks(subset)
    _validate_blocks_df(df)

    # Addition of features
    for fn in get_feature_pipeline():
        df = fn(df)

    out_path = write_enriched_blocks(df, subset)
    return out_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich comment blocks with classification features.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--subset", choices=SUBSETS, help="Process exactly one subset.")
    g.add_argument("--all", action="store_true", help="Process all subsets.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.all:
        for subset in SUBSETS:
            out = enrich_subset(subset)
            print(f"[comment-metrics] Wrote enriched blocks: {out}")
    else:
        out = enrich_subset(args.subset)
        print(f"[comment-metrics] Wrote enriched blocks: {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())