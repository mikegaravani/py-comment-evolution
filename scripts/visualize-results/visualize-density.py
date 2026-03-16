"""
Create quick density figures for the thesis.

USAGE:
    python scripts/visualize-results/visualize-density.py --subset core
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


SUBSETS = ["core", "core_plus_tests", "tests_only", "all_py"]


def input_repo_density_path(subset: str) -> Path:
    return Path("results") / subset / "density" / "repo_level_density.csv"


def input_group_density_path(subset: str) -> Path:
    return Path("results") / subset / "density" / "group_level_density.csv"


def output_figure_dir(subset: str) -> Path:
    return Path("results") / subset / "density" / "figures"


def read_repo_density(subset: str) -> pd.DataFrame:
    path = input_repo_density_path(subset)
    if not path.exists():
        raise FileNotFoundError(f"Missing repo density CSV: {path}")
    return pd.read_csv(path)


def read_group_density(subset: str) -> pd.DataFrame:
    path = input_group_density_path(subset)
    if not path.exists():
        raise FileNotFoundError(f"Missing group density CSV: {path}")
    return pd.read_csv(path)


def save_current_figure(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()


def make_repo_block_count_bar(repo_df: pd.DataFrame, subset: str) -> Path:
    """
    Bar chart:
    x = repo
    y = blocks_per_kloc
    color = era/group

    Repos are sorted left-to-right in increasing comment density.
    """
    df = repo_df.copy()

    # Sort repos by increasing density
    df = df.sort_values(["blocks_per_kloc", "repo"], ascending=[True, True]).reset_index(drop=True)

    color_map = {
        "old_2000s": "#4C78A8",
        "new_2020s": "#F58518",
    }
    colors = [color_map.get(group, "#999999") for group in df["group"]]

    plt.figure(figsize=(12, 6))
    plt.bar(df["repo"], df["blocks_per_kloc"], color=colors)
    plt.xlabel("Repo")
    plt.ylabel("Comment blocks per 1,000 LOC")
    plt.title(f"Comment density by repo ({subset})")
    plt.xticks(rotation=45, ha="right")

    handles = [
        plt.Rectangle((0, 0), 1, 1, color=color_map["old_2000s"], label="2000s"),
        plt.Rectangle((0, 0), 1, 1, color=color_map["new_2020s"], label="2020s"),
    ]
    plt.legend(handles=handles, title="Era")

    out_path = output_figure_dir(subset) / "repo_blocks_per_kloc_bar.png"
    save_current_figure(out_path)
    return out_path


def make_era_blocks_per_kloc_boxplot(repo_df: pd.DataFrame, subset: str) -> Path:
    """
    Boxplot by era with individual repo dots overlaid.
    y = blocks_per_kloc
    """
    df = repo_df.copy()

    era_order = ["old_2000s", "new_2020s"]
    labels = ["2000s", "2020s"]

    data = [df.loc[df["group"] == era, "blocks_per_kloc"].dropna().tolist() for era in era_order]

    plt.figure(figsize=(7, 6))
    plt.boxplot(data, labels=labels, patch_artist=False)

    # Overlay repo points with small horizontal jitter
    for i, era in enumerate(era_order, start=1):
        era_values = df.loc[df["group"] == era, "blocks_per_kloc"].dropna().tolist()
        if not era_values:
            continue

        # deterministic small spread
        if len(era_values) == 1:
            xs = [i]
        else:
            step = 0.12 / (len(era_values) - 1)
            xs = [i - 0.06 + j * step for j in range(len(era_values))]

        plt.scatter(xs, era_values, alpha=0.9)

    plt.ylabel("Blocks per 1000 LOC")
    plt.xlabel("Era")
    plt.title(f"Comment density by era ({subset})")

    out_path = output_figure_dir(subset) / "era_blocks_per_kloc_boxplot.png"
    save_current_figure(out_path)
    return out_path


def make_era_block_kind_stacked_bar(group_df: pd.DataFrame, subset: str) -> Path:
    """
    Stacked bar chart per era using block-kind ratios.
    """
    df = group_df.copy()

    era_order = ["old_2000s", "new_2020s"]
    label_map = {
        "old_2000s": "2000s",
        "new_2020s": "2020s",
    }

    df = df.set_index("group").reindex(era_order).reset_index()

    x_labels = [label_map.get(g, g) for g in df["group"]]

    inline_vals = df["inline_block_ratio"].fillna(0.0).tolist()
    singleton_vals = df["full_line_singleton_block_ratio"].fillna(0.0).tolist()
    block_vals = df["full_line_block_block_ratio"].fillna(0.0).tolist()

    plt.figure(figsize=(7, 6))
    plt.bar(x_labels, inline_vals, label="Inline")
    plt.bar(x_labels, singleton_vals, bottom=inline_vals, label="Full-line singleton")

    bottom_for_blocks = [a + b for a, b in zip(inline_vals, singleton_vals)]
    plt.bar(x_labels, block_vals, bottom=bottom_for_blocks, label="Full-line block")

    plt.ylabel("Share of comment blocks")
    plt.xlabel("Era")
    plt.title(f"Block-kind composition by era ({subset})")
    plt.ylim(0, 1.0)
    plt.legend()

    out_path = output_figure_dir(subset) / "era_block_kind_stacked_bar.png"
    save_current_figure(out_path)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Create density figures for thesis.")
    parser.add_argument("--subset", required=True, choices=SUBSETS)
    args = parser.parse_args()

    subset = args.subset

    repo_df = read_repo_density(subset)
    group_df = read_group_density(subset)

    out1 = make_repo_block_count_bar(repo_df, subset)
    out2 = make_era_blocks_per_kloc_boxplot(repo_df, subset)
    out3 = make_era_block_kind_stacked_bar(group_df, subset)

    print(f"[figures, subset: {subset}] Wrote → {out1}")
    print(f"[figures, subset: {subset}] Wrote → {out2}")
    print(f"[figures, subset: {subset}] Wrote → {out3}")


if __name__ == "__main__":
    main()