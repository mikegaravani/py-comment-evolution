"""
Build comment block parquet files from comment parquet inputs.

USAGE:
    # Process all subsets:
    python scripts/comment-metrics/build-comment-blocks/build-comment-blocks.py --all

    # Process one subset:
    python scripts/comment-metrics/build-comment-blocks/build-comment-blocks.py --subset <subset>
"""
from __future__ import annotations

import argparse

from io_blocks import SUBSETS, read_comments, write_blocks
from blockify import build_comment_blocks


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build comment block parquet files from tokenized comment parquet inputs."
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--subset", choices=SUBSETS, help="Process exactly one subset.")
    g.add_argument("--all", action="store_true", help="Process all subsets.")
    return p.parse_args()


def run_for_subset(subset: str) -> None:
    print(f"[comment-metrics] Reading: {subset}")
    df = read_comments(subset)

    print(f"[comment-metrics] Blockifying: {subset} (rows={len(df):,})")
    df_blocks = build_comment_blocks(
        df_comments=df,
        subset=subset,
    )

    out_path = write_blocks(df_blocks, subset)
    print(f"[comment-metrics] Wrote blocks: {out_path} (blocks={len(df_blocks):,})")


def main() -> int:
    args = parse_args()

    if args.all:
        for subset in SUBSETS:
            run_for_subset(subset)
    else:
        run_for_subset(args.subset)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())