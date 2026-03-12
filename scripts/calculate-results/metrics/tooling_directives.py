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


def compute_tooling_directive_metrics(
    blocks_df: pd.DataFrame,
    subset: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute tooling-directive metrics from enriched comment blocks.

    Outputs:
    - repo level
    - group level

    Focus:
    - total block count
    - blocks with any tooling directive
    - blocks with each specific tooling directive
    - ratios for overall and specific directives
    """

    df = blocks_df.copy()

    repo_level_df = (
        df.groupby(REPO_GROUP_COLS, as_index=False)
        .agg(
            total_blocks=("block_id", "count"),

            tooling_blocks=("td_has_tooling_directive", "sum"),

            noqa_blocks=("td_has_noqa", "sum"),
            pragma_blocks=("td_has_pragma", "sum"),
            type_ignore_blocks=("td_has_type_ignore", "sum"),
            pylint_blocks=("td_has_pylint", "sum"),
            mypy_blocks=("td_has_mypy", "sum"),
            fmt_blocks=("td_has_fmt", "sum"),
            encoding_blocks=("td_has_encoding", "sum"),
        )
    )

    repo_level_df["subset"] = subset

    repo_level_df["tooling_block_ratio"] = (
        repo_level_df["tooling_blocks"] / repo_level_df["total_blocks"]
    )

    repo_level_df["noqa_block_ratio"] = (
        repo_level_df["noqa_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["pragma_block_ratio"] = (
        repo_level_df["pragma_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["type_ignore_block_ratio"] = (
        repo_level_df["type_ignore_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["pylint_block_ratio"] = (
        repo_level_df["pylint_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["mypy_block_ratio"] = (
        repo_level_df["mypy_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["fmt_block_ratio"] = (
        repo_level_df["fmt_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["encoding_block_ratio"] = (
        repo_level_df["encoding_blocks"] / repo_level_df["total_blocks"]
    )

    repo_level_out = repo_level_df[
        [
            "subset",
            "repo",
            "group",
            "release",
            "total_blocks",

            "tooling_blocks",
            "noqa_blocks",
            "pragma_blocks",
            "type_ignore_blocks",
            "pylint_blocks",
            "mypy_blocks",
            "fmt_blocks",
            "encoding_blocks",

            "tooling_block_ratio",
            "noqa_block_ratio",
            "pragma_block_ratio",
            "type_ignore_block_ratio",
            "pylint_block_ratio",
            "mypy_block_ratio",
            "fmt_block_ratio",
            "encoding_block_ratio",
        ]
    ].copy()

    group_level_df = (
        repo_level_out.groupby(GROUP_GROUP_COLS, as_index=False)
        .agg(
            repo_count=("repo", "count"),
            total_blocks=("total_blocks", "sum"),

            tooling_blocks=("tooling_blocks", "sum"),
            noqa_blocks=("noqa_blocks", "sum"),
            pragma_blocks=("pragma_blocks", "sum"),
            type_ignore_blocks=("type_ignore_blocks", "sum"),
            pylint_blocks=("pylint_blocks", "sum"),
            mypy_blocks=("mypy_blocks", "sum"),
            fmt_blocks=("fmt_blocks", "sum"),
            encoding_blocks=("encoding_blocks", "sum"),

            mean_repo_tooling_block_ratio=("tooling_block_ratio", "mean"),
            median_repo_tooling_block_ratio=("tooling_block_ratio", "median"),

            mean_repo_noqa_block_ratio=("noqa_block_ratio", "mean"),
            median_repo_noqa_block_ratio=("noqa_block_ratio", "median"),

            mean_repo_pragma_block_ratio=("pragma_block_ratio", "mean"),
            median_repo_pragma_block_ratio=("pragma_block_ratio", "median"),

            mean_repo_type_ignore_block_ratio=("type_ignore_block_ratio", "mean"),
            median_repo_type_ignore_block_ratio=("type_ignore_block_ratio", "median"),

            mean_repo_pylint_block_ratio=("pylint_block_ratio", "mean"),
            median_repo_pylint_block_ratio=("pylint_block_ratio", "median"),

            mean_repo_mypy_block_ratio=("mypy_block_ratio", "mean"),
            median_repo_mypy_block_ratio=("mypy_block_ratio", "median"),

            mean_repo_fmt_block_ratio=("fmt_block_ratio", "mean"),
            median_repo_fmt_block_ratio=("fmt_block_ratio", "median"),

            mean_repo_encoding_block_ratio=("encoding_block_ratio", "mean"),
            median_repo_encoding_block_ratio=("encoding_block_ratio", "median"),
        )
    )

    group_level_df["subset"] = subset

    group_level_df["tooling_block_ratio"] = (
        group_level_df["tooling_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["noqa_block_ratio"] = (
        group_level_df["noqa_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["pragma_block_ratio"] = (
        group_level_df["pragma_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["type_ignore_block_ratio"] = (
        group_level_df["type_ignore_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["pylint_block_ratio"] = (
        group_level_df["pylint_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["mypy_block_ratio"] = (
        group_level_df["mypy_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["fmt_block_ratio"] = (
        group_level_df["fmt_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["encoding_block_ratio"] = (
        group_level_df["encoding_blocks"] / group_level_df["total_blocks"]
    )

    group_level_out = group_level_df[
        [
            "subset",
            "group",
            "repo_count",
            "total_blocks",

            "tooling_blocks",
            "noqa_blocks",
            "pragma_blocks",
            "type_ignore_blocks",
            "pylint_blocks",
            "mypy_blocks",
            "fmt_blocks",
            "encoding_blocks",

            "tooling_block_ratio",
            "noqa_block_ratio",
            "pragma_block_ratio",
            "type_ignore_block_ratio",
            "pylint_block_ratio",
            "mypy_block_ratio",
            "fmt_block_ratio",
            "encoding_block_ratio",

            "mean_repo_tooling_block_ratio",
            "median_repo_tooling_block_ratio",

            "mean_repo_noqa_block_ratio",
            "median_repo_noqa_block_ratio",

            "mean_repo_pragma_block_ratio",
            "median_repo_pragma_block_ratio",

            "mean_repo_type_ignore_block_ratio",
            "median_repo_type_ignore_block_ratio",

            "mean_repo_pylint_block_ratio",
            "median_repo_pylint_block_ratio",

            "mean_repo_mypy_block_ratio",
            "median_repo_mypy_block_ratio",

            "mean_repo_fmt_block_ratio",
            "median_repo_fmt_block_ratio",

            "mean_repo_encoding_block_ratio",
            "median_repo_encoding_block_ratio",
        ]
    ].copy()

    return repo_level_out, group_level_out