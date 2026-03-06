from __future__ import annotations

from pathlib import Path
import pandas as pd


SUBSETS = ["core", "core_plus_tests", "tests_only", "all_py"]


def input_parquet_path(subset: str) -> Path:
    return Path("data/processed/comment_blocks") / subset / "comment_blocks.parquet"


def output_parquet_path(subset: str) -> Path:
    return Path("data/processed/comment_blocks_enriched") / subset / "comment_blocks_enriched.parquet"


def read__blocks(subset: str) -> pd.DataFrame:
    in_path = input_parquet_path(subset)
    if not in_path.exists():
        raise FileNotFoundError(f"Missing input parquet: {in_path}")
    return pd.read_parquet(in_path)


def write_enriched_blocks(df_enriched: pd.DataFrame, subset: str) -> Path:
    out_path = output_parquet_path(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_enriched.to_parquet(out_path, index=False, engine="pyarrow", compression="zstd")
    return out_path