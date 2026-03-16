from __future__ import annotations

import pandas as pd



GROUP_COLS = ["subset", "repo", "group", "release"]


def compute_repo_level_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates enriched comment blocks to repo-level metrics.
    One output row per repo.
    """

    df = df.copy()

    df["is_inline"] = df["block_kind"] == "inline"
    df["is_full_line"] = ~df["is_inline"]

    grouped = df.groupby(GROUP_COLS)

    repo_df = grouped.agg(
        total_blocks=("block_id", "count"),

        inline_blocks=("is_inline", "sum"),
        full_line_blocks=("is_full_line", "sum"),

        total_comment_lines=("n_lines", "sum"),

        mean_block_char_len=("block_char_len", "mean"),
        median_block_char_len=("block_char_len", "median"),

        mean_block_word_len=("block_word_len", "mean"),
        median_block_word_len=("block_word_len", "median"),

        mean_block_lines=("n_lines", "mean"),
        median_block_lines=("n_lines", "median"),

        # legal
        legal_blocks=("lh_has_legal_signal", "sum"),

        # annotation markers
        annotation_blocks=("am_has_annotation_marker", "sum"),

        # tooling
        tooling_blocks=("td_has_tooling_directive", "sum"),

        # linguistic
        punctuation_end_blocks=("lf_has_punctuation_end", "sum"),
        question_blocks=("lf_is_question", "sum"),
        exclamation_blocks=("lf_is_exclamation", "sum"),
        url_blocks=("lf_has_url", "sum"),
        separator_blocks=("lf_is_separator", "sum"),

    ).reset_index()

    # Derived proportions
    repo_df["inline_block_ratio"] = repo_df["inline_blocks"] / repo_df["total_blocks"]
    repo_df["full_line_block_ratio"] = repo_df["full_line_blocks"] / repo_df["total_blocks"]

    repo_df["legal_block_ratio"] = repo_df["legal_blocks"] / repo_df["total_blocks"]
    repo_df["annotation_block_ratio"] = repo_df["annotation_blocks"] / repo_df["total_blocks"]
    repo_df["tooling_block_ratio"] = repo_df["tooling_blocks"] / repo_df["total_blocks"]

    repo_df["punctuation_end_ratio"] = repo_df["punctuation_end_blocks"] / repo_df["total_blocks"]
    repo_df["question_ratio"] = repo_df["question_blocks"] / repo_df["total_blocks"]
    repo_df["exclamation_ratio"] = repo_df["exclamation_blocks"] / repo_df["total_blocks"]
    repo_df["url_ratio"] = repo_df["url_blocks"] / repo_df["total_blocks"]
    repo_df["separator_ratio"] = repo_df["separator_blocks"] / repo_df["total_blocks"]

    return repo_df