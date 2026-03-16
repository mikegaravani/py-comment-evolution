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


def compute_annotation_marker_metrics(
    blocks_df: pd.DataFrame,
    subset: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute annotation-marker metrics from enriched comment blocks.

    Outputs:
    - repo level
    - group level

    Focus:
    - total block count
    - blocks with any annotation marker
    - blocks with each specific annotation marker
    - ratios for overall and specific markers
    """

    df = blocks_df.copy()

    repo_level_df = (
        df.groupby(REPO_GROUP_COLS, as_index=False)
        .agg(
            total_blocks=("block_id", "count"),

            annotation_blocks=("am_has_annotation_marker", "sum"),

            todo_blocks=("am_has_todo", "sum"),
            fixme_blocks=("am_has_fixme", "sum"),
            xxx_blocks=("am_has_xxx", "sum"),
            hack_blocks=("am_has_hack", "sum"),
            bug_blocks=("am_has_bug", "sum"),
            note_blocks=("am_has_note", "sum"),
        )
    )

    repo_level_df["subset"] = subset

    repo_level_df["annotation_block_ratio"] = (
        repo_level_df["annotation_blocks"] / repo_level_df["total_blocks"]
    )

    repo_level_df["todo_block_ratio"] = (
        repo_level_df["todo_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["fixme_block_ratio"] = (
        repo_level_df["fixme_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["xxx_block_ratio"] = (
        repo_level_df["xxx_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["hack_block_ratio"] = (
        repo_level_df["hack_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["bug_block_ratio"] = (
        repo_level_df["bug_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["note_block_ratio"] = (
        repo_level_df["note_blocks"] / repo_level_df["total_blocks"]
    )

    repo_level_out = repo_level_df[
        [
            "subset",
            "repo",
            "group",
            "release",
            "total_blocks",

            "annotation_blocks",
            "todo_blocks",
            "fixme_blocks",
            "xxx_blocks",
            "hack_blocks",
            "bug_blocks",
            "note_blocks",

            "annotation_block_ratio",
            "todo_block_ratio",
            "fixme_block_ratio",
            "xxx_block_ratio",
            "hack_block_ratio",
            "bug_block_ratio",
            "note_block_ratio",
        ]
    ].copy()

    group_level_df = (
        repo_level_out.groupby(GROUP_GROUP_COLS, as_index=False)
        .agg(
            repo_count=("repo", "count"),
            total_blocks=("total_blocks", "sum"),

            annotation_blocks=("annotation_blocks", "sum"),
            todo_blocks=("todo_blocks", "sum"),
            fixme_blocks=("fixme_blocks", "sum"),
            xxx_blocks=("xxx_blocks", "sum"),
            hack_blocks=("hack_blocks", "sum"),
            bug_blocks=("bug_blocks", "sum"),
            note_blocks=("note_blocks", "sum"),

            mean_repo_annotation_block_ratio=("annotation_block_ratio", "mean"),
            median_repo_annotation_block_ratio=("annotation_block_ratio", "median"),

            mean_repo_todo_block_ratio=("todo_block_ratio", "mean"),
            median_repo_todo_block_ratio=("todo_block_ratio", "median"),

            mean_repo_fixme_block_ratio=("fixme_block_ratio", "mean"),
            median_repo_fixme_block_ratio=("fixme_block_ratio", "median"),

            mean_repo_xxx_block_ratio=("xxx_block_ratio", "mean"),
            median_repo_xxx_block_ratio=("xxx_block_ratio", "median"),

            mean_repo_hack_block_ratio=("hack_block_ratio", "mean"),
            median_repo_hack_block_ratio=("hack_block_ratio", "median"),

            mean_repo_bug_block_ratio=("bug_block_ratio", "mean"),
            median_repo_bug_block_ratio=("bug_block_ratio", "median"),

            mean_repo_note_block_ratio=("note_block_ratio", "mean"),
            median_repo_note_block_ratio=("note_block_ratio", "median"),
        )
    )

    group_level_df["subset"] = subset

    group_level_df["annotation_block_ratio"] = (
        group_level_df["annotation_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["todo_block_ratio"] = (
        group_level_df["todo_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["fixme_block_ratio"] = (
        group_level_df["fixme_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["xxx_block_ratio"] = (
        group_level_df["xxx_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["hack_block_ratio"] = (
        group_level_df["hack_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["bug_block_ratio"] = (
        group_level_df["bug_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["note_block_ratio"] = (
        group_level_df["note_blocks"] / group_level_df["total_blocks"]
    )

    group_level_out = group_level_df[
        [
            "subset",
            "group",
            "repo_count",
            "total_blocks",

            "annotation_blocks",
            "todo_blocks",
            "fixme_blocks",
            "xxx_blocks",
            "hack_blocks",
            "bug_blocks",
            "note_blocks",

            "annotation_block_ratio",
            "todo_block_ratio",
            "fixme_block_ratio",
            "xxx_block_ratio",
            "hack_block_ratio",
            "bug_block_ratio",
            "note_block_ratio",

            "mean_repo_annotation_block_ratio",
            "median_repo_annotation_block_ratio",

            "mean_repo_todo_block_ratio",
            "median_repo_todo_block_ratio",

            "mean_repo_fixme_block_ratio",
            "median_repo_fixme_block_ratio",

            "mean_repo_xxx_block_ratio",
            "median_repo_xxx_block_ratio",

            "mean_repo_hack_block_ratio",
            "median_repo_hack_block_ratio",

            "mean_repo_bug_block_ratio",
            "median_repo_bug_block_ratio",

            "mean_repo_note_block_ratio",
            "median_repo_note_block_ratio",
        ]
    ].copy()

    return repo_level_out, group_level_out