from __future__ import annotations

import pandas as pd


SUBSET_FLAG_MAP = {
    "core": "subset_core",
    "core_plus_tests": "subset_core_plus_tests",
    "tests_only": "subset_tests_only",
    "all_py": "subset_all_py",
}


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
    df = df[df[subset_flag] == True].copy()

    # EXCLUDING EMPTY FILES!!!!!
    df = df[df["loc_total"] > 0].copy()

    return df


def _aggregate_block_counts_by_file(blocks_df: pd.DataFrame) -> pd.DataFrame:
    if blocks_df.empty:
        return pd.DataFrame(columns=["file_id", "block_count"])

    out = (
        blocks_df.groupby("file_id", as_index=False)
        .agg(block_count=("block_id", "count"))
    )
    return out


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

    # Warnable mismatch info can be inspected by caller if needed
    block_file_ids = set(block_counts_df["file_id"].unique()) if not block_counts_df.empty else set()
    file_index_file_ids = set(files_df["file_id"].unique())
    orphan_block_file_ids = block_file_ids - file_index_file_ids

    file_level_df = files_df.merge(
        block_counts_df,
        on="file_id",
        how="left",
        validate="one_to_one",
    )

    file_level_df["block_count"] = file_level_df["block_count"].fillna(0).astype(int)
    file_level_df["blocks_per_file"] = file_level_df["block_count"].astype(float)
    file_level_df["blocks_per_kloc"] = (
        file_level_df["block_count"] / (file_level_df["loc_total"] / 1000.0)
    )

    file_level_df["subset"] = subset

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
        ]
    ].copy()

    repo_level_df = (
        file_level_df.groupby(REPO_GROUP_COLS, as_index=False)
        .agg(
            file_count=("file_id", "count"),
            loc_total=("loc_total", "sum"),
            block_count=("block_count", "sum"),
        )
    )

    repo_level_df["subset"] = subset
    repo_level_df["blocks_per_file"] = repo_level_df["block_count"] / repo_level_df["file_count"]
    repo_level_df["blocks_per_kloc"] = (
        repo_level_df["block_count"] / (repo_level_df["loc_total"] / 1000.0)
    )

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
        ]
    ].copy()

    group_level_df = (
        repo_level_out.groupby(GROUP_GROUP_COLS, as_index=False)
        .agg(
            repo_count=("repo", "count"),
            file_count=("file_count", "sum"),
            loc_total=("loc_total", "sum"),
            block_count=("block_count", "sum"),
            mean_repo_blocks_per_file=("blocks_per_file", "mean"),
            median_repo_blocks_per_file=("blocks_per_file", "median"),
            mean_repo_blocks_per_kloc=("blocks_per_kloc", "mean"),
            median_repo_blocks_per_kloc=("blocks_per_kloc", "median"),
        )
    )

    group_level_df["subset"] = subset
    group_level_df["blocks_per_file"] = group_level_df["block_count"] / group_level_df["file_count"]
    group_level_df["blocks_per_kloc"] = (
        group_level_df["block_count"] / (group_level_df["loc_total"] / 1000.0)
    )

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
            "mean_repo_blocks_per_file",
            "median_repo_blocks_per_file",
            "mean_repo_blocks_per_kloc",
            "median_repo_blocks_per_kloc",
        ]
    ].copy()

    if orphan_block_file_ids:
        print(
            f"[density, subset: {subset}] Warning: "
            f"{len(orphan_block_file_ids)} file_id(s) appear in comment blocks "
            f"but not in subset-filtered file_index. They were ignored."
        )

    return file_level_out, repo_level_out, group_level_out