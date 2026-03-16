from __future__ import annotations

import pandas as pd

# map for subset flags in file_index.parquet
SUBSET_FLAG_MAP = {
    "core": "subset_core",
    "core_plus_tests": "subset_core_plus_tests",
    "tests_only": "subset_tests_only",
    "all_py": "subset_all_py",
}

# kinds of blocks
BLOCK_KINDS = [
    "inline",
    "full_line_singleton",
    "full_line_block",
]

FILE_GROUP_COLS = [
    "repo",
    "group",
    "release",
    "file_id",
    "path_rel",
]

REPO_GROUP_COLS = [
    "repo",
    "group",
    "release",
]

GROUP_GROUP_COLS = [
    "group",
]


def _filter_file_index_for_subset(file_index_df: pd.DataFrame, subset: str) -> pd.DataFrame:
    if subset not in SUBSET_FLAG_MAP:
        raise ValueError(f"Unknown subset: {subset}")

    subset_flag = SUBSET_FLAG_MAP[subset]
    if subset_flag not in file_index_df.columns:
        raise KeyError(f"Missing subset flag column in file index: {subset_flag}")

    df = file_index_df.copy()

    # normalization of file index naming (repo vs name)
    if "repo" not in df.columns:
        if "name" in df.columns:
            df = df.rename(columns={"name": "repo"})
        else:
            raise KeyError("file_index.parquet must contain either 'repo' or 'name'")

    # file_index is the authority
    df = df[df[subset_flag] == True].copy() # noqa: E712

    # EXCLUDING EMPTY FILES!!!!!
    df = df[df["loc_total"] > 0].copy()

    return df


def _aggregate_block_counts_by_file(blocks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns one row per file_id with:
    - total block_count
    - block-kind counts
    """
    if blocks_df.empty:
        return pd.DataFrame(
            columns=[
                "file_id",
                "block_count",
                "inline_block_count",
                "full_line_singleton_block_count",
                "full_line_block_block_count",
            ]
        )

    total_counts = (
        blocks_df.groupby("file_id", as_index=False)
        .agg(block_count=("block_id", "count"))
    )

    kind_counts = (
        blocks_df.groupby(["file_id", "block_kind"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    # ensure all expected block kinds exist as columns
    for kind in BLOCK_KINDS:
        if kind not in kind_counts.columns:
            kind_counts[kind] = 0

    kind_counts = kind_counts.rename(
        columns={
            "inline": "inline_block_count",
            "full_line_singleton": "full_line_singleton_block_count",
            "full_line_block": "full_line_block_block_count",
        }
    )

    out = total_counts.merge(kind_counts, on="file_id", how="left", validate="one_to_one")

    count_cols = [
        "block_count",
        "inline_block_count",
        "full_line_singleton_block_count",
        "full_line_block_block_count",
    ]
    for col in count_cols:
        out[col] = out[col].fillna(0).astype(int)

    return out


def _add_block_kind_ratios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    ratio_specs = [
        ("inline_block_count", "inline_block_ratio"),
        ("full_line_singleton_block_count", "full_line_singleton_block_ratio"),
        ("full_line_block_block_count", "full_line_block_block_ratio"),
    ]

    for count_col, ratio_col in ratio_specs:
        df[ratio_col] = 0.0
        nonzero_mask = df["block_count"] > 0
        df.loc[nonzero_mask, ratio_col] = (
            df.loc[nonzero_mask, count_col] / df.loc[nonzero_mask, "block_count"]
        )

    return df


def compute_density_metrics(
    file_index_df: pd.DataFrame,
    blocks_df: pd.DataFrame,
    subset: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compute comment density metrics at:
    - file level
    - repo level
    - group level

    Rules:
    - file_index.parquet is the authority for file inclusion
    - subset membership is determined from file_index subset boolean flags
    - files with zero LOC are excluded
    - files with no comment blocks are retained with block_count = 0
    """

    files_df = _filter_file_index_for_subset(file_index_df, subset)
    block_counts_df = _aggregate_block_counts_by_file(blocks_df)

    # check for orphan file_ids
    block_file_ids = set(block_counts_df["file_id"].unique()) if not block_counts_df.empty else set()
    file_index_file_ids = set(files_df["file_id"].unique())
    orphan_block_file_ids = block_file_ids - file_index_file_ids

    file_level_df = files_df.merge(
        block_counts_df,
        on="file_id",
        how="left",
        validate="one_to_one",
    )

    count_cols = [
        "block_count",
        "inline_block_count",
        "full_line_singleton_block_count",
        "full_line_block_block_count",
    ]
    for col in count_cols:
        file_level_df[col] = file_level_df[col].fillna(0).astype(int)

    file_level_df["block_count"] = file_level_df["block_count"].fillna(0).astype(int)
    file_level_df["blocks_per_file"] = file_level_df["block_count"].astype(float)
    file_level_df["blocks_per_kloc"] = (
        file_level_df["block_count"] / (file_level_df["loc_total"] / 1000.0)
    )

    file_level_df["subset"] = subset

    file_level_df = _add_block_kind_ratios(file_level_df)

    file_level_out = file_level_df[
        [
            "subset",
            "repo",
            "group",
            "release",
            "file_id",
            "path_rel",
            "loc_total",
            "block_count",
            "blocks_per_file",
            "blocks_per_kloc",
            "inline_block_count",
            "full_line_singleton_block_count",
            "full_line_block_block_count",
            "inline_block_ratio",
            "full_line_singleton_block_ratio",
            "full_line_block_block_ratio",
        ]
    ].copy()

    repo_level_df = (
        file_level_df.groupby(REPO_GROUP_COLS, as_index=False)
        .agg(
            file_count=("file_id", "count"),
            loc_total=("loc_total", "sum"),
            block_count=("block_count", "sum"),
            inline_block_count=("inline_block_count", "sum"),
            full_line_singleton_block_count=("full_line_singleton_block_count", "sum"),
            full_line_block_block_count=("full_line_block_block_count", "sum"),
        )
    )

    repo_level_df["subset"] = subset
    repo_level_df["blocks_per_file"] = repo_level_df["block_count"] / repo_level_df["file_count"]
    repo_level_df["blocks_per_kloc"] = (
        repo_level_df["block_count"] / (repo_level_df["loc_total"] / 1000.0)
    )
    repo_level_df = _add_block_kind_ratios(repo_level_df)

    repo_level_out = repo_level_df[
        [
            "subset",
            "repo",
            "group",
            "release",
            "file_count",
            "loc_total",
            "block_count",
            "blocks_per_file",
            "blocks_per_kloc",
            "inline_block_count",
            "full_line_singleton_block_count",
            "full_line_block_block_count",
            "inline_block_ratio",
            "full_line_singleton_block_ratio",
            "full_line_block_block_ratio",
        ]
    ].copy()

    group_level_df = (
        repo_level_out.groupby(GROUP_GROUP_COLS, as_index=False)
        .agg(
            repo_count=("repo", "count"),
            file_count=("file_count", "sum"),
            loc_total=("loc_total", "sum"),
            block_count=("block_count", "sum"),
            inline_block_count=("inline_block_count", "sum"),
            full_line_singleton_block_count=("full_line_singleton_block_count", "sum"),
            full_line_block_block_count=("full_line_block_block_count", "sum"),
            mean_repo_blocks_per_file=("blocks_per_file", "mean"),
            median_repo_blocks_per_file=("blocks_per_file", "median"),
            mean_repo_blocks_per_kloc=("blocks_per_kloc", "mean"),
            median_repo_blocks_per_kloc=("blocks_per_kloc", "median"),
            mean_repo_inline_block_ratio=("inline_block_ratio", "mean"),
            median_repo_inline_block_ratio=("inline_block_ratio", "median"),
            mean_repo_full_line_singleton_block_ratio=("full_line_singleton_block_ratio", "mean"),
            median_repo_full_line_singleton_block_ratio=("full_line_singleton_block_ratio", "median"),
            mean_repo_full_line_block_block_ratio=("full_line_block_block_ratio", "mean"),
            median_repo_full_line_block_block_ratio=("full_line_block_block_ratio", "median"),
        )
    )

    group_level_df["subset"] = subset
    group_level_df["blocks_per_file"] = group_level_df["block_count"] / group_level_df["file_count"]
    group_level_df["blocks_per_kloc"] = (
        group_level_df["block_count"] / (group_level_df["loc_total"] / 1000.0)
    )
    group_level_df = _add_block_kind_ratios(group_level_df)

    group_level_out = group_level_df[
        [
            "subset",
            "group",
            "repo_count",
            "file_count",
            "loc_total",
            "block_count",
            "blocks_per_file",
            "blocks_per_kloc",
            "inline_block_count",
            "full_line_singleton_block_count",
            "full_line_block_block_count",
            "inline_block_ratio",
            "full_line_singleton_block_ratio",
            "full_line_block_block_ratio",
            "mean_repo_blocks_per_file",
            "median_repo_blocks_per_file",
            "mean_repo_blocks_per_kloc",
            "median_repo_blocks_per_kloc",
            "mean_repo_inline_block_ratio",
            "median_repo_inline_block_ratio",
            "mean_repo_full_line_singleton_block_ratio",
            "median_repo_full_line_singleton_block_ratio",
            "mean_repo_full_line_block_block_ratio",
            "median_repo_full_line_block_block_ratio",
        ]
    ].copy()

    if orphan_block_file_ids:
        print(
            f"[density, subset: {subset}] Warning: "
            f"{len(orphan_block_file_ids)} file_id(s) appear in comment blocks "
            f"but not in subset-filtered file_index. They were ignored."
        )

    return file_level_out, repo_level_out, group_level_out