from __future__ import annotations

import pandas as pd


REPO_GROUP_COLS = [
    "repo",
    "group",
    "release",
]

GROUP_GROUP_COLS = [
    "group",
]


VALID_BLOCK_KINDS = [
    "inline",
    "full_line_singleton",
    "full_line_block",
]


def compute_structure_metrics(
    blocks_df: pd.DataFrame,
    subset: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute structure-oriented comment metrics from enriched comment blocks.

    Outputs:
    - repo level
    - group level

    Focus:
    - distribution of block_kind
    - block_char_len
    - block_word_len
    """

    df = blocks_df.copy()

    unknown_kinds = sorted(set(df["block_kind"].dropna().unique()) - set(VALID_BLOCK_KINDS))
    if unknown_kinds:
        print(
            f"[structure, subset: {subset}] Warning: "
            f"unknown block_kind values found: {unknown_kinds}"
        )

    df["is_inline"] = df["block_kind"] == "inline"
    df["is_full_line_singleton"] = df["block_kind"] == "full_line_singleton"
    df["is_full_line_block"] = df["block_kind"] == "full_line_block"

    repo_level_df = (
        df.groupby(REPO_GROUP_COLS, as_index=False)
        .agg(
            total_blocks=("block_id", "count"),

            inline_blocks=("is_inline", "sum"),
            full_line_singleton_blocks=("is_full_line_singleton", "sum"),
            full_line_block_blocks=("is_full_line_block", "sum"),

            mean_block_char_len=("block_char_len", "mean"),
            median_block_char_len=("block_char_len", "median"),

            mean_block_word_len=("block_word_len", "mean"),
            median_block_word_len=("block_word_len", "median"),

            mean_inline_block_char_len=("block_char_len", lambda s: s[df.loc[s.index, "is_inline"]].mean()),
            median_inline_block_char_len=("block_char_len", lambda s: s[df.loc[s.index, "is_inline"]].median()),

            mean_full_line_singleton_char_len=("block_char_len", lambda s: s[df.loc[s.index, "is_full_line_singleton"]].mean()),
            median_full_line_singleton_char_len=("block_char_len", lambda s: s[df.loc[s.index, "is_full_line_singleton"]].median()),

            mean_full_line_block_char_len=("block_char_len", lambda s: s[df.loc[s.index, "is_full_line_block"]].mean()),
            median_full_line_block_char_len=("block_char_len", lambda s: s[df.loc[s.index, "is_full_line_block"]].median()),

            mean_inline_block_word_len=("block_word_len", lambda s: s[df.loc[s.index, "is_inline"]].mean()),
            median_inline_block_word_len=("block_word_len", lambda s: s[df.loc[s.index, "is_inline"]].median()),

            mean_full_line_singleton_word_len=("block_word_len", lambda s: s[df.loc[s.index, "is_full_line_singleton"]].mean()),
            median_full_line_singleton_word_len=("block_word_len", lambda s: s[df.loc[s.index, "is_full_line_singleton"]].median()),

            mean_full_line_block_word_len=("block_word_len", lambda s: s[df.loc[s.index, "is_full_line_block"]].mean()),
            median_full_line_block_word_len=("block_word_len", lambda s: s[df.loc[s.index, "is_full_line_block"]].median()),
        )
    )

    repo_level_df["subset"] = subset

    repo_level_df["inline_block_ratio"] = repo_level_df["inline_blocks"] / repo_level_df["total_blocks"]
    repo_level_df["full_line_singleton_block_ratio"] = (
        repo_level_df["full_line_singleton_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["full_line_block_ratio"] = (
        repo_level_df["full_line_block_blocks"] / repo_level_df["total_blocks"]
    )

    repo_level_out = repo_level_df[
        [
            "subset",
            "repo",
            "group",
            "release",
            "total_blocks",

            "inline_blocks",
            "full_line_singleton_blocks",
            "full_line_block_blocks",

            "inline_block_ratio",
            "full_line_singleton_block_ratio",
            "full_line_block_ratio",

            "mean_block_char_len",
            "median_block_char_len",
            "mean_block_word_len",
            "median_block_word_len",

            "mean_inline_block_char_len",
            "median_inline_block_char_len",
            "mean_full_line_singleton_char_len",
            "median_full_line_singleton_char_len",
            "mean_full_line_block_char_len",
            "median_full_line_block_char_len",

            "mean_inline_block_word_len",
            "median_inline_block_word_len",
            "mean_full_line_singleton_word_len",
            "median_full_line_singleton_word_len",
            "mean_full_line_block_word_len",
            "median_full_line_block_word_len",
        ]
    ].copy()

    group_level_df = (
        repo_level_out.groupby(GROUP_GROUP_COLS, as_index=False)
        .agg(
            repo_count=("repo", "count"),
            total_blocks=("total_blocks", "sum"),

            inline_blocks=("inline_blocks", "sum"),
            full_line_singleton_blocks=("full_line_singleton_blocks", "sum"),
            full_line_block_blocks=("full_line_block_blocks", "sum"),

            mean_repo_inline_block_ratio=("inline_block_ratio", "mean"),
            median_repo_inline_block_ratio=("inline_block_ratio", "median"),

            mean_repo_full_line_singleton_block_ratio=("full_line_singleton_block_ratio", "mean"),
            median_repo_full_line_singleton_block_ratio=("full_line_singleton_block_ratio", "median"),

            mean_repo_full_line_block_ratio=("full_line_block_ratio", "mean"),
            median_repo_full_line_block_ratio=("full_line_block_ratio", "median"),

            mean_repo_mean_block_char_len=("mean_block_char_len", "mean"),
            median_repo_mean_block_char_len=("mean_block_char_len", "median"),

            mean_repo_median_block_char_len=("median_block_char_len", "mean"),
            median_repo_median_block_char_len=("median_block_char_len", "median"),

            mean_repo_mean_block_word_len=("mean_block_word_len", "mean"),
            median_repo_mean_block_word_len=("mean_block_word_len", "median"),

            mean_repo_median_block_word_len=("median_block_word_len", "mean"),
            median_repo_median_block_word_len=("median_block_word_len", "median"),

            mean_repo_mean_inline_block_char_len=("mean_inline_block_char_len", "mean"),
            median_repo_mean_inline_block_char_len=("mean_inline_block_char_len", "median"),

            mean_repo_mean_full_line_singleton_char_len=("mean_full_line_singleton_char_len", "mean"),
            median_repo_mean_full_line_singleton_char_len=("mean_full_line_singleton_char_len", "median"),

            mean_repo_mean_full_line_block_char_len=("mean_full_line_block_char_len", "mean"),
            median_repo_mean_full_line_block_char_len=("mean_full_line_block_char_len", "median"),

            mean_repo_mean_inline_block_word_len=("mean_inline_block_word_len", "mean"),
            median_repo_mean_inline_block_word_len=("mean_inline_block_word_len", "median"),

            mean_repo_mean_full_line_singleton_word_len=("mean_full_line_singleton_word_len", "mean"),
            median_repo_mean_full_line_singleton_word_len=("mean_full_line_singleton_word_len", "median"),

            mean_repo_mean_full_line_block_word_len=("mean_full_line_block_word_len", "mean"),
            median_repo_mean_full_line_block_word_len=("mean_full_line_block_word_len", "median"),
        )
    )

    group_level_df["subset"] = subset

    group_level_df["inline_block_ratio"] = group_level_df["inline_blocks"] / group_level_df["total_blocks"]
    group_level_df["full_line_singleton_block_ratio"] = (
        group_level_df["full_line_singleton_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["full_line_block_ratio"] = (
        group_level_df["full_line_block_blocks"] / group_level_df["total_blocks"]
    )

    group_level_out = group_level_df[
        [
            "subset",
            "group",
            "repo_count",
            "total_blocks",

            "inline_blocks",
            "full_line_singleton_blocks",
            "full_line_block_blocks",

            "inline_block_ratio",
            "full_line_singleton_block_ratio",
            "full_line_block_ratio",

            "mean_repo_inline_block_ratio",
            "median_repo_inline_block_ratio",
            "mean_repo_full_line_singleton_block_ratio",
            "median_repo_full_line_singleton_block_ratio",
            "mean_repo_full_line_block_ratio",
            "median_repo_full_line_block_ratio",

            "mean_repo_mean_block_char_len",
            "median_repo_mean_block_char_len",
            "mean_repo_median_block_char_len",
            "median_repo_median_block_char_len",

            "mean_repo_mean_block_word_len",
            "median_repo_mean_block_word_len",
            "mean_repo_median_block_word_len",
            "median_repo_median_block_word_len",

            "mean_repo_mean_inline_block_char_len",
            "median_repo_mean_inline_block_char_len",
            "mean_repo_mean_full_line_singleton_char_len",
            "median_repo_mean_full_line_singleton_char_len",
            "mean_repo_mean_full_line_block_char_len",
            "median_repo_mean_full_line_block_char_len",

            "mean_repo_mean_inline_block_word_len",
            "median_repo_mean_inline_block_word_len",
            "mean_repo_mean_full_line_singleton_word_len",
            "median_repo_mean_full_line_singleton_word_len",
            "mean_repo_mean_full_line_block_word_len",
            "median_repo_mean_full_line_block_word_len",
        ]
    ].copy()

    return repo_level_out, group_level_out