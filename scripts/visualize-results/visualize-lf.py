"""
Create quick linguistic-feature figures for the thesis.

USAGE:
    python scripts/visualize-results/visualize-lf.py --subset core
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


SUBSETS = ["core", "core_plus_tests", "tests_only", "all_py"]


def input_group_linguistic_features_path(subset: str) -> Path:
    return Path("results") / subset / "linguistic_features" / "group_level_linguistic_features.csv"

def input_repo_linguistic_features_path(subset: str) -> Path:
    return Path("results") / subset / "linguistic_features" / "repo_level_linguistic_features.csv"

def output_figure_dir(subset: str) -> Path:
    return Path("results") / subset / "linguistic_features" / "figures"


def read_group_linguistic_features(subset: str) -> pd.DataFrame:
    path = input_group_linguistic_features_path(subset)
    if not path.exists():
        raise FileNotFoundError(f"Missing group linguistic-features CSV: {path}")
    return pd.read_csv(path)

def read_repo_linguistic_features(subset: str) -> pd.DataFrame:
    path = input_repo_linguistic_features_path(subset)
    if not path.exists():
        raise FileNotFoundError(f"Missing repo linguistic-features CSV: {path}")
    return pd.read_csv(path)



def save_current_figure(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()


def _era_color_map() -> dict[str, str]:
    return {
        "old_2000s": "#4C78A8",
        "new_2020s": "#F58518",
    }


def make_punctuation_pattern_ratio_double_bar(group_df: pd.DataFrame, subset: str) -> Path:
    """
    Grouped/double bar chart:
    x = punctuation pattern
    y = ratio
    color = era

    Each bar is annotated with:
        <blocks>/<total_blocks>
    """

    df = group_df.copy()

    era_order = ["old_2000s", "new_2020s"]
    label_map = {
        "old_2000s": "2000s",
        "new_2020s": "2020s",
    }
    color_map = _era_color_map()

    df = df.set_index("group").reindex(era_order).reset_index()

    metrics = [
        ("ends_with_period_block_ratio", "ends_with_period_blocks", "Ends with period"),
        ("question_block_ratio", "question_blocks", "Question"),
        ("exclamation_block_ratio", "exclamation_blocks", "Exclamation"),
        ("punctuation_end_block_ratio", "punctuation_end_blocks", "Any punctuation end"),
    ]

    x = list(range(len(metrics)))
    width = 0.38

    old_vals = []
    new_vals = []
    old_labels = []
    new_labels = []

    old_total = int(df.loc[df["group"] == "old_2000s", "total_blocks"].iloc[0])
    new_total = int(df.loc[df["group"] == "new_2020s", "total_blocks"].iloc[0])

    for ratio_col, count_col, _label in metrics:
        old_ratio = df.loc[df["group"] == "old_2000s", ratio_col].iloc[0]
        new_ratio = df.loc[df["group"] == "new_2020s", ratio_col].iloc[0]

        old_count = int(df.loc[df["group"] == "old_2000s", count_col].iloc[0])
        new_count = int(df.loc[df["group"] == "new_2020s", count_col].iloc[0])

        old_vals.append(old_ratio)
        new_vals.append(new_ratio)

        old_labels.append(f"{old_count}/{old_total}")
        new_labels.append(f"{new_count}/{new_total}")

    x_old = [i - width / 2 for i in x]
    x_new = [i + width / 2 for i in x]
    x_labels = [label for _r, _c, label in metrics]

    plt.figure(figsize=(10, 6))

    bars_old = plt.bar(
        x_old, old_vals, width=width,
        color=color_map["old_2000s"], label=label_map["old_2000s"]
    )

    bars_new = plt.bar(
        x_new, new_vals, width=width,
        color=color_map["new_2020s"], label=label_map["new_2020s"]
    )

    # overlay fraction labels
    for bar, label in zip(bars_old, old_labels):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2 - 0.03,
            height + 0.002,
            label,
            ha="center",
            va="bottom",
            fontsize=9
        )

    for bar, label in zip(bars_new, new_labels):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2 + 0.03,
            height + 0.002,
            label,
            ha="center",
            va="bottom",
            fontsize=9
        )

    plt.xticks(x, x_labels)
    plt.xlabel("Punctuation pattern")
    plt.ylabel("Rate (blocks / total blocks)")
    plt.title(f"Punctuation patterns by era ({subset})")
    plt.legend(title="Era")

    out_path = output_figure_dir(subset) / "punctuation_pattern_ratio_double_bar.png"
    save_current_figure(out_path)
    return out_path


def make_repo_imperative_to_descriptive_ratio_bar(repo_df: pd.DataFrame, subset: str) -> Path:
    """
    Bar chart:
    x = repo
    y = imperative-to-descriptive ratio
    color = era/group

    Ratio is defined as:
        starts_with_imperative_verb_blocks / starts_with_descriptive_verb_blocks

    Repos with zero descriptive blocks are excluded from the plot.
    Repos are sorted left-to-right in increasing ratio.
    """
    df = repo_df.copy()
    color_map = _era_color_map()

    df["imperative_to_descriptive_ratio"] = (
        df["starts_with_imperative_verb_blocks"] / df["starts_with_descriptive_verb_blocks"]
    )

    df = df.replace([float("inf"), float("-inf")], pd.NA)
    df = df.dropna(subset=["imperative_to_descriptive_ratio"]).copy()

    df = df.sort_values(
        ["imperative_to_descriptive_ratio", "repo"],
        ascending=[True, True],
    ).reset_index(drop=True)

    colors = [color_map.get(group, "#999999") for group in df["group"]]

    plt.figure(figsize=(12, 6))
    plt.bar(df["repo"], df["imperative_to_descriptive_ratio"], color=colors)

    plt.xlabel("Repo")
    plt.ylabel("Imperative / descriptive block ratio")
    plt.title(f"Imperative-to-descriptive comment ratio by repo ({subset})")
    plt.xticks(rotation=45, ha="right")

    handles = [
        plt.Rectangle((0, 0), 1, 1, color=color_map["old_2000s"], label="2000s"),
        plt.Rectangle((0, 0), 1, 1, color=color_map["new_2020s"], label="2020s"),
    ]
    plt.legend(handles=handles, title="Era")

    out_path = output_figure_dir(subset) / "repo_imperative_to_descriptive_ratio_bar.png"
    save_current_figure(out_path)
    return out_path

def make_group_imperative_descriptive_ratio_double_bar(group_df: pd.DataFrame, subset: str) -> Path:
    """
    Grouped/double bar chart:
    x = linguistic start type
    y = ratio over total blocks
    color = era

    Each bar is annotated with:
        <blocks>/<total_blocks>
    """

    df = group_df.copy()

    era_order = ["old_2000s", "new_2020s"]
    label_map = {
        "old_2000s": "2000s",
        "new_2020s": "2020s",
    }
    color_map = _era_color_map()

    df = df.set_index("group").reindex(era_order).reset_index()

    metrics = [
        (
            "starts_with_imperative_verb_block_ratio",
            "starts_with_imperative_verb_blocks",
            "Imperative",
        ),
        (
            "starts_with_descriptive_verb_block_ratio",
            "starts_with_descriptive_verb_blocks",
            "Descriptive",
        ),
    ]

    x = list(range(len(metrics)))
    width = 0.38

    old_vals = []
    new_vals = []
    old_labels = []
    new_labels = []

    old_total = int(df.loc[df["group"] == "old_2000s", "total_blocks"].iloc[0])
    new_total = int(df.loc[df["group"] == "new_2020s", "total_blocks"].iloc[0])

    for ratio_col, count_col, _label in metrics:
        old_ratio = df.loc[df["group"] == "old_2000s", ratio_col].iloc[0]
        new_ratio = df.loc[df["group"] == "new_2020s", ratio_col].iloc[0]

        old_count = int(df.loc[df["group"] == "old_2000s", count_col].iloc[0])
        new_count = int(df.loc[df["group"] == "new_2020s", count_col].iloc[0])

        old_vals.append(old_ratio)
        new_vals.append(new_ratio)

        old_labels.append(f"{old_count}/{old_total}")
        new_labels.append(f"{new_count}/{new_total}")

    x_old = [i - width / 2 for i in x]
    x_new = [i + width / 2 for i in x]
    x_labels = [label for _r, _c, label in metrics]

    plt.figure(figsize=(8, 6))

    bars_old = plt.bar(
        x_old,
        old_vals,
        width=width,
        color=color_map["old_2000s"],
        label=label_map["old_2000s"],
    )

    bars_new = plt.bar(
        x_new,
        new_vals,
        width=width,
        color=color_map["new_2020s"],
        label=label_map["new_2020s"],
    )

    # overlay count/total labels
    for bar, label in zip(bars_old, old_labels):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2 - 0.01,
            height + 0.002,
            label,
            ha="center",
            va="bottom",
            fontsize=9,
        )

    for bar, label in zip(bars_new, new_labels):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2 + 0.01,
            height + 0.002,
            label,
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.xticks(x, x_labels)
    plt.xlabel("Comment start type")
    plt.ylabel("Ratio of total comment blocks")
    plt.title(f"Imperative and descriptive starts by era ({subset})")
    plt.legend(title="Era")

    out_path = output_figure_dir(subset) / "group_imperative_descriptive_ratio_double_bar.png"
    save_current_figure(out_path)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Create linguistic-feature figures for thesis.")
    parser.add_argument("--subset", required=True, choices=SUBSETS)
    args = parser.parse_args()

    subset = args.subset
    repo_df = read_repo_linguistic_features(subset)
    group_df = read_group_linguistic_features(subset)

    out_path = make_punctuation_pattern_ratio_double_bar(group_df, subset)
    # out2 = make_repo_imperative_to_descriptive_ratio_bar(repo_df, subset) # discarded
    out3 = make_group_imperative_descriptive_ratio_double_bar(group_df, subset)
    print(f"[figures, subset: {subset}] Wrote → {out_path}")
    # print(f"[figures, subset: {subset}] Wrote → {out2}")
    print(f"[figures, subset: {subset}] Wrote → {out3}")


if __name__ == "__main__":
    main()