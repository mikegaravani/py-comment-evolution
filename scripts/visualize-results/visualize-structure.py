"""
Create quick structure figures for the thesis.

USAGE:
    python scripts/visualize-results/visualize-structure.py --subset core
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


SUBSETS = ["core", "core_plus_tests", "tests_only", "all_py"]


def input_repo_structure_path(subset: str) -> Path:
    return Path("results") / subset / "structure" / "repo_level_structure.csv"


def input_group_structure_path(subset: str) -> Path:
    return Path("results") / subset / "structure" / "group_level_structure.csv"


def output_figure_dir(subset: str) -> Path:
    return Path("results") / subset / "structure" / "figures"


def read_repo_structure(subset: str) -> pd.DataFrame:
    path = input_repo_structure_path(subset)
    if not path.exists():
        raise FileNotFoundError(f"Missing repo structure CSV: {path}")
    return pd.read_csv(path)


def read_group_structure(subset: str) -> pd.DataFrame:
    path = input_group_structure_path(subset)
    if not path.exists():
        raise FileNotFoundError(f"Missing group structure CSV: {path}")
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


def make_repo_mean_block_char_len_histogram(repo_df: pd.DataFrame, subset: str) -> Path:
    """
    Histogram of repo-level mean block char length, split by era.
    x = mean block char length per repo
    y = number of repos
    """
    df = repo_df.copy()
    color_map = _era_color_map()

    old_vals = df.loc[df["group"] == "old_2000s", "mean_block_char_len"].dropna()
    new_vals = df.loc[df["group"] == "new_2020s", "mean_block_char_len"].dropna()

    plt.figure(figsize=(8, 6))
    plt.hist(old_vals, bins=8, alpha=0.7, label="2000s", color=color_map["old_2000s"])
    plt.hist(new_vals, bins=8, alpha=0.7, label="2020s", color=color_map["new_2020s"])
    plt.xlabel("Mean block character length per repo")
    plt.ylabel("Number of repos")
    plt.title(f"Distribution of average block character length ({subset})")
    plt.legend(title="Era")

    out_path = output_figure_dir(subset) / "repo_mean_block_char_len_histogram.png"
    save_current_figure(out_path)
    return out_path


def make_repo_mean_block_word_len_histogram(repo_df: pd.DataFrame, subset: str) -> Path:
    """
    Histogram of repo-level mean block word length, split by era.
    x = mean block word length per repo
    y = number of repos
    """
    df = repo_df.copy()
    color_map = _era_color_map()

    old_vals = df.loc[df["group"] == "old_2000s", "mean_block_word_len"].dropna()
    new_vals = df.loc[df["group"] == "new_2020s", "mean_block_word_len"].dropna()

    plt.figure(figsize=(8, 6))
    plt.hist(old_vals, bins=8, alpha=0.7, label="2000s", color=color_map["old_2000s"])
    plt.hist(new_vals, bins=8, alpha=0.7, label="2020s", color=color_map["new_2020s"])
    plt.xlabel("Mean block word length per repo")
    plt.ylabel("Number of repos")
    plt.title(f"Distribution of average block word length ({subset})")
    plt.legend(title="Era")

    out_path = output_figure_dir(subset) / "repo_mean_block_word_len_histogram.png"
    save_current_figure(out_path)
    return out_path


def make_era_block_kind_ratio_stacked_bar(group_df: pd.DataFrame, subset: str) -> Path:
    """
    Stacked bar chart by era for pooled block kind ratios.
    """
    df = group_df.copy()

    era_order = ["old_2000s", "new_2020s"]
    label_map = {
        "old_2000s": "2000s",
        "new_2020s": "2020s",
    }

    df = df.set_index("group").reindex(era_order).reset_index()

    x_labels = [label_map[g] for g in df["group"]]

    inline_vals = df["inline_block_ratio"].fillna(0)
    singleton_vals = df["full_line_singleton_block_ratio"].fillna(0)
    block_vals = df["full_line_block_ratio"].fillna(0)

    plt.figure(figsize=(8, 6))
    plt.bar(x_labels, inline_vals, label="Inline")
    plt.bar(x_labels, singleton_vals, bottom=inline_vals, label="Full-line singleton")
    plt.bar(
        x_labels,
        block_vals,
        bottom=inline_vals + singleton_vals,
        label="Full-line block",
    )

    plt.xlabel("Era")
    plt.ylabel("Proportion of comment blocks")
    plt.title(f"Block kind composition by era ({subset})")
    plt.legend(title="Block kind")

    out_path = output_figure_dir(subset) / "era_block_kind_ratio_stacked_bar.png"
    save_current_figure(out_path)
    return out_path


def make_repo_inline_ratio_bar(repo_df: pd.DataFrame, subset: str) -> Path:
    """
    Bar chart:
    x = repo
    y = inline_block_ratio
    color = era/group

    Repos are sorted left-to-right in increasing inline proportion.
    """
    df = repo_df.copy()
    color_map = _era_color_map()

    df = df.sort_values(["inline_block_ratio", "repo"], ascending=[True, True]).reset_index(drop=True)
    colors = [color_map.get(group, "#999999") for group in df["group"]]

    plt.figure(figsize=(12, 6))
    plt.bar(df["repo"], df["inline_block_ratio"], color=colors)
    plt.xlabel("Repo")
    plt.ylabel("Inline block proportion")
    plt.title(f"Inline comment proportion by repo ({subset})")
    plt.xticks(rotation=45, ha="right")

    handles = [
        plt.Rectangle((0, 0), 1, 1, color=color_map["old_2000s"], label="2000s"),
        plt.Rectangle((0, 0), 1, 1, color=color_map["new_2020s"], label="2020s"),
    ]
    plt.legend(handles=handles, title="Era")

    out_path = output_figure_dir(subset) / "repo_inline_block_ratio_bar.png"
    save_current_figure(out_path)
    return out_path


def make_block_kind_char_len_boxplots(repo_df: pd.DataFrame, subset: str) -> Path:
    """
    Three side-by-side boxplot areas:
    - mean inline block char len per repo
    - mean full-line singleton char len per repo
    - mean full-line block char len per repo

    Within each area: 2000s vs 2020s
    """
    df = repo_df.copy()

    metrics = [
        ("mean_inline_block_char_len", "Inline"),
        ("mean_full_line_singleton_char_len", "Full-line singleton"),
        ("mean_full_line_block_char_len", "Full-line block"),
    ]

    positions = [1, 2, 4, 5, 7, 8]
    data = []
    labels = []

    for metric_col, label in metrics:
        old_vals = df.loc[df["group"] == "old_2000s", metric_col].dropna().tolist()
        new_vals = df.loc[df["group"] == "new_2020s", metric_col].dropna().tolist()
        data.extend([old_vals, new_vals])
        labels.extend([f"{label}\n2000s", f"{label}\n2020s"])

    plt.figure(figsize=(12, 6))
    plt.boxplot(data, positions=positions, widths=0.7, patch_artist=False)

    # overlay deterministic jittered repo points
    for pos, vals in zip(positions, data):
        if not vals:
            continue
        if len(vals) == 1:
            xs = [pos]
        else:
            step = 0.14 / (len(vals) - 1)
            xs = [pos - 0.07 + j * step for j in range(len(vals))]
        plt.scatter(xs, vals, alpha=0.9)

    plt.xticks(positions, labels)
    plt.ylabel("Mean character length per repo")
    plt.xlabel("Block kind and era")
    plt.title(f"Average character length by block kind and era ({subset})")

    out_path = output_figure_dir(subset) / "block_kind_char_len_boxplots.png"
    save_current_figure(out_path)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Create structure figures for thesis.")
    parser.add_argument("--subset", required=True, choices=SUBSETS)
    args = parser.parse_args()

    subset = args.subset

    repo_df = read_repo_structure(subset)
    group_df = read_group_structure(subset)

    # out1 = make_repo_mean_block_char_len_histogram(repo_df, subset)
    # out2 = make_repo_mean_block_word_len_histogram(repo_df, subset)
    out3 = make_era_block_kind_ratio_stacked_bar(group_df, subset)
    out4 = make_repo_inline_ratio_bar(repo_df, subset)
    out5 = make_block_kind_char_len_boxplots(repo_df, subset)

    # print(f"[figures, subset: {subset}] Wrote → {out1}")
    # print(f"[figures, subset: {subset}] Wrote → {out2}")
    print(f"[figures, subset: {subset}] Wrote → {out3}")
    print(f"[figures, subset: {subset}] Wrote → {out4}")
    print(f"[figures, subset: {subset}] Wrote → {out5}")


if __name__ == "__main__":
    main()