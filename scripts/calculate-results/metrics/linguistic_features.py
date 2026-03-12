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


def _filter_linguistic_analysis_blocks(blocks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only comment blocks that are not:
    - shebangs
    - legal
    - annotation markers
    - tooling directives
    """

    df = blocks_df.copy()

    mask = (
        (df["sb_is_shebang"] == False)
        & (df["lh_has_legal_signal"] == False)
        & (df["am_has_annotation_marker"] == False)
        & (df["td_has_tooling_directive"] == False)
    )

    return df.loc[mask].copy()


def _safe_emoticon_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(x) for x in value]
    return []


def compute_linguistic_feature_metrics(
    blocks_df: pd.DataFrame,
    subset: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute linguistic-feature metrics from enriched comment blocks after filtering out:
    - shebangs
    - legal blocks
    - annotation-marker blocks
    - tooling-directive blocks

    Outputs:
    - repo level
    - group level

    Focus:
    - filtered block count
    - counts and ratios for linguistic features
    - emoticon instance and inventory summaries
    """

    df = _filter_linguistic_analysis_blocks(blocks_df)

    df["is_inline"] = df["block_kind"] == "inline"
    df["is_full_line_singleton"] = df["block_kind"] == "full_line_singleton"
    df["is_full_line_block"] = df["block_kind"] == "full_line_block"

    df["inline_punctuation_end"] = df["is_inline"] & df["lf_has_punctuation_end"]
    df["full_line_singleton_punctuation_end"] = (
        df["is_full_line_singleton"] & df["lf_has_punctuation_end"]
    )
    df["full_line_block_punctuation_end"] = (
        df["is_full_line_block"] & df["lf_has_punctuation_end"]
    )

    df["inline_ends_with_period"] = df["is_inline"] & df["lf_ends_with_period"]
    df["full_line_singleton_ends_with_period"] = (
        df["is_full_line_singleton"] & df["lf_ends_with_period"]
    )
    df["full_line_block_ends_with_period"] = (
        df["is_full_line_block"] & df["lf_ends_with_period"]
    )

    df["inline_is_question"] = df["is_inline"] & df["lf_is_question"]
    df["full_line_singleton_is_question"] = (
        df["is_full_line_singleton"] & df["lf_is_question"]
    )
    df["full_line_block_is_question"] = (
        df["is_full_line_block"] & df["lf_is_question"]
    )

    df["inline_is_exclamation"] = df["is_inline"] & df["lf_is_exclamation"]
    df["full_line_singleton_is_exclamation"] = (
        df["is_full_line_singleton"] & df["lf_is_exclamation"]
    )
    df["full_line_block_is_exclamation"] = (
        df["is_full_line_block"] & df["lf_is_exclamation"]
    )

    df["emoticon_instance_count"] = df["lf_emoticons_found"].apply(
        lambda x: len(_safe_emoticon_list(x))
    )

    repo_level_df = (
        df.groupby(REPO_GROUP_COLS, as_index=False)
        .agg(
            total_blocks=("block_id", "count"),

            inline_blocks=("is_inline", "sum"),
            full_line_singleton_blocks=("is_full_line_singleton", "sum"),
            full_line_block_blocks=("is_full_line_block", "sum"),

            inline_punctuation_end_blocks=("inline_punctuation_end", "sum"),
            full_line_singleton_punctuation_end_blocks=("full_line_singleton_punctuation_end", "sum"),
            full_line_block_punctuation_end_blocks=("full_line_block_punctuation_end", "sum"),
            inline_ends_with_period_blocks=("inline_ends_with_period", "sum"),
            full_line_singleton_ends_with_period_blocks=(
                "full_line_singleton_ends_with_period",
                "sum",
            ),
            full_line_block_ends_with_period_blocks=(
                "full_line_block_ends_with_period",
                "sum",
            ),
            inline_question_blocks=("inline_is_question", "sum"),
            full_line_singleton_question_blocks=(
                "full_line_singleton_is_question",
                "sum",
            ),
            full_line_block_question_blocks=(
                "full_line_block_is_question",
                "sum",
            ),
            inline_exclamation_blocks=("inline_is_exclamation", "sum"),
            full_line_singleton_exclamation_blocks=(
                "full_line_singleton_is_exclamation",
                "sum",            ),
            full_line_block_exclamation_blocks=(
                "full_line_block_is_exclamation",
                "sum",
            ),

            punctuation_end_blocks=("lf_has_punctuation_end", "sum"),
            ends_with_period_blocks=("lf_ends_with_period", "sum"),
            question_blocks=("lf_is_question", "sum"),
            exclamation_blocks=("lf_is_exclamation", "sum"),
            all_caps_blocks=("lf_is_all_caps", "sum"),
            starts_with_lowercase_blocks=("lf_starts_with_lowercase", "sum"),
            starts_with_imperative_verb_blocks=("lf_starts_with_imperative_verb", "sum"),
            starts_with_descriptive_verb_blocks=("lf_starts_with_descriptive_verb", "sum"),
            no_alphanumeric_blocks=("lf_has_no_alphanumeric", "sum"),
            empty_blocks=("lf_is_empty", "sum"),
            separator_blocks=("lf_is_separator", "sum"),
            url_blocks=("lf_has_url", "sum"),
            number_blocks=("lf_has_number", "sum"),
            emoticon_blocks=("lf_has_emoticon", "sum"),
            parentheses_blocks=("lf_has_parentheses", "sum"),
            linguistic_feature_blocks=("lf_has_linguistic_feature", "sum"),

            emoticon_instance_count=("emoticon_instance_count", "sum"),
        )
    )

    repo_level_df["subset"] = subset

    repo_level_df["punctuation_end_block_ratio"] = (
        repo_level_df["punctuation_end_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["ends_with_period_block_ratio"] = (
        repo_level_df["ends_with_period_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["question_block_ratio"] = (
        repo_level_df["question_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["exclamation_block_ratio"] = (
        repo_level_df["exclamation_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["all_caps_block_ratio"] = (
        repo_level_df["all_caps_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["starts_with_lowercase_block_ratio"] = (
        repo_level_df["starts_with_lowercase_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["starts_with_imperative_verb_block_ratio"] = (
        repo_level_df["starts_with_imperative_verb_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["starts_with_descriptive_verb_block_ratio"] = (
        repo_level_df["starts_with_descriptive_verb_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["no_alphanumeric_block_ratio"] = (
        repo_level_df["no_alphanumeric_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["empty_block_ratio"] = (
        repo_level_df["empty_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["separator_block_ratio"] = (
        repo_level_df["separator_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["url_block_ratio"] = (
        repo_level_df["url_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["number_block_ratio"] = (
        repo_level_df["number_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["emoticon_block_ratio"] = (
        repo_level_df["emoticon_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["parentheses_block_ratio"] = (
        repo_level_df["parentheses_blocks"] / repo_level_df["total_blocks"]
    )
    repo_level_df["linguistic_feature_block_ratio"] = (
        repo_level_df["linguistic_feature_blocks"] / repo_level_df["total_blocks"]
    )

    # unique emoticon inventory by repo
    repo_emoticon_df = (
        df.groupby(REPO_GROUP_COLS)["lf_emoticons_found"]
        .apply(lambda s: sorted({emo for lst in s for emo in _safe_emoticon_list(lst)}))
        .reset_index(name="unique_emoticons")
    )
    repo_emoticon_df["unique_emoticons_count"] = repo_emoticon_df["unique_emoticons"].apply(len)
    repo_emoticon_df["unique_emoticons"] = repo_emoticon_df["unique_emoticons"].apply(
        lambda xs: " | ".join(xs)
    )

    repo_level_df = repo_level_df.merge(
        repo_emoticon_df,
        on=REPO_GROUP_COLS,
        how="left",
        validate="one_to_one",
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

            "inline_punctuation_end_blocks",
            "full_line_singleton_punctuation_end_blocks",
            "full_line_block_punctuation_end_blocks",
            "inline_ends_with_period_blocks",
            "full_line_singleton_ends_with_period_blocks",
            "full_line_block_ends_with_period_blocks",
            "inline_question_blocks",
            "full_line_singleton_question_blocks",
            "full_line_block_question_blocks",
            "inline_exclamation_blocks",
            "full_line_singleton_exclamation_blocks",
            "full_line_block_exclamation_blocks",

            "punctuation_end_blocks",
            "ends_with_period_blocks",
            "question_blocks",
            "exclamation_blocks",
            "all_caps_blocks",
            "starts_with_lowercase_blocks",
            "starts_with_imperative_verb_blocks",
            "starts_with_descriptive_verb_blocks",
            "no_alphanumeric_blocks",
            "empty_blocks",
            "separator_blocks",
            "url_blocks",
            "number_blocks",
            "emoticon_blocks",
            "parentheses_blocks",
            "linguistic_feature_blocks",

            "punctuation_end_block_ratio",
            "ends_with_period_block_ratio",
            "question_block_ratio",
            "exclamation_block_ratio",
            "all_caps_block_ratio",
            "starts_with_lowercase_block_ratio",
            "starts_with_imperative_verb_block_ratio",
            "starts_with_descriptive_verb_block_ratio",
            "no_alphanumeric_block_ratio",
            "empty_block_ratio",
            "separator_block_ratio",
            "url_block_ratio",
            "number_block_ratio",
            "emoticon_block_ratio",
            "parentheses_block_ratio",
            "linguistic_feature_block_ratio",

            "emoticon_instance_count",
            "unique_emoticons_count",
            "unique_emoticons",
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

            inline_punctuation_end_blocks=("inline_punctuation_end_blocks", "sum"),
            full_line_singleton_punctuation_end_blocks=("full_line_singleton_punctuation_end_blocks", "sum"),
            full_line_block_punctuation_end_blocks=("full_line_block_punctuation_end_blocks", "sum"),
            inline_ends_with_period_blocks=("inline_ends_with_period_blocks", "sum"),
            full_line_singleton_ends_with_period_blocks=(
                "full_line_singleton_ends_with_period_blocks",
                "sum",
            ),
            full_line_block_ends_with_period_blocks=(
                "full_line_block_ends_with_period_blocks",
                "sum",
            ),
            inline_question_blocks=("inline_question_blocks", "sum"),
            full_line_singleton_question_blocks=(
                "full_line_singleton_question_blocks",
                "sum",
            ),
            full_line_block_question_blocks=(
                "full_line_block_question_blocks",
                "sum",
            ),
            inline_exclamation_blocks=("inline_exclamation_blocks", "sum"),
            full_line_singleton_exclamation_blocks=(
                "full_line_singleton_exclamation_blocks",
                "sum",
            ),
            full_line_block_exclamation_blocks=(
                "full_line_block_exclamation_blocks",
                "sum",
            ),

            punctuation_end_blocks=("punctuation_end_blocks", "sum"),
            ends_with_period_blocks=("ends_with_period_blocks", "sum"),
            question_blocks=("question_blocks", "sum"),
            exclamation_blocks=("exclamation_blocks", "sum"),
            all_caps_blocks=("all_caps_blocks", "sum"),
            starts_with_lowercase_blocks=("starts_with_lowercase_blocks", "sum"),
            starts_with_imperative_verb_blocks=("starts_with_imperative_verb_blocks", "sum"),
            starts_with_descriptive_verb_blocks=("starts_with_descriptive_verb_blocks", "sum"),
            no_alphanumeric_blocks=("no_alphanumeric_blocks", "sum"),
            empty_blocks=("empty_blocks", "sum"),
            separator_blocks=("separator_blocks", "sum"),
            url_blocks=("url_blocks", "sum"),
            number_blocks=("number_blocks", "sum"),
            emoticon_blocks=("emoticon_blocks", "sum"),
            parentheses_blocks=("parentheses_blocks", "sum"),
            linguistic_feature_blocks=("linguistic_feature_blocks", "sum"),

            emoticon_instance_count=("emoticon_instance_count", "sum"),
        )
    )

    group_level_df["subset"] = subset

    group_level_df["punctuation_end_block_ratio"] = (
        group_level_df["punctuation_end_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["ends_with_period_block_ratio"] = (
        group_level_df["ends_with_period_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["question_block_ratio"] = (
        group_level_df["question_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["exclamation_block_ratio"] = (
        group_level_df["exclamation_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["all_caps_block_ratio"] = (
        group_level_df["all_caps_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["starts_with_lowercase_block_ratio"] = (
        group_level_df["starts_with_lowercase_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["starts_with_imperative_verb_block_ratio"] = (
        group_level_df["starts_with_imperative_verb_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["starts_with_descriptive_verb_block_ratio"] = (
        group_level_df["starts_with_descriptive_verb_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["no_alphanumeric_block_ratio"] = (
        group_level_df["no_alphanumeric_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["empty_block_ratio"] = (
        group_level_df["empty_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["separator_block_ratio"] = (
        group_level_df["separator_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["url_block_ratio"] = (
        group_level_df["url_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["number_block_ratio"] = (
        group_level_df["number_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["emoticon_block_ratio"] = (
        group_level_df["emoticon_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["parentheses_block_ratio"] = (
        group_level_df["parentheses_blocks"] / group_level_df["total_blocks"]
    )
    group_level_df["linguistic_feature_block_ratio"] = (
        group_level_df["linguistic_feature_blocks"] / group_level_df["total_blocks"]
    )

    # unique emoticon inventory by group
    group_emoticon_df = (
        df.groupby(GROUP_GROUP_COLS)["lf_emoticons_found"]
        .apply(lambda s: sorted({emo for lst in s for emo in _safe_emoticon_list(lst)}))
        .reset_index(name="unique_emoticons")
    )
    group_emoticon_df["unique_emoticons_count"] = group_emoticon_df["unique_emoticons"].apply(len)
    group_emoticon_df["unique_emoticons"] = group_emoticon_df["unique_emoticons"].apply(
        lambda xs: " | ".join(xs)
    )

    group_level_df = group_level_df.merge(
        group_emoticon_df,
        on=GROUP_GROUP_COLS,
        how="left",
        validate="one_to_one",
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
            "inline_punctuation_end_blocks",
            "full_line_singleton_punctuation_end_blocks",
            "full_line_block_punctuation_end_blocks",
            "inline_ends_with_period_blocks",
            "full_line_singleton_ends_with_period_blocks",
            "full_line_block_ends_with_period_blocks",
            "inline_question_blocks",
            "full_line_singleton_question_blocks",
            "full_line_block_question_blocks",
            "inline_exclamation_blocks",
            "full_line_singleton_exclamation_blocks",
            "full_line_block_exclamation_blocks",

            "punctuation_end_blocks",
            "ends_with_period_blocks",
            "question_blocks",
            "exclamation_blocks",
            "all_caps_blocks",
            "starts_with_lowercase_blocks",
            "starts_with_imperative_verb_blocks",
            "starts_with_descriptive_verb_blocks",
            "no_alphanumeric_blocks",
            "empty_blocks",
            "separator_blocks",
            "url_blocks",
            "number_blocks",
            "emoticon_blocks",
            "parentheses_blocks",
            "linguistic_feature_blocks",

            "punctuation_end_block_ratio",
            "ends_with_period_block_ratio",
            "question_block_ratio",
            "exclamation_block_ratio",
            "all_caps_block_ratio",
            "starts_with_lowercase_block_ratio",
            "starts_with_imperative_verb_block_ratio",
            "starts_with_descriptive_verb_block_ratio",
            "no_alphanumeric_block_ratio",
            "empty_block_ratio",
            "separator_block_ratio",
            "url_block_ratio",
            "number_block_ratio",
            "emoticon_block_ratio",
            "parentheses_block_ratio",
            "linguistic_feature_block_ratio",

            "emoticon_instance_count",
            "unique_emoticons_count",
            "unique_emoticons",
        ]
    ].copy()

    return repo_level_out, group_level_out