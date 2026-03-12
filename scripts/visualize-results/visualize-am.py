"""
Create quick annotation marker figures for the thesis.

USAGE:
    python scripts/visualize-results/visualize-am.py --subset core
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


SUBSETS = ["core", "core_plus_tests", "tests_only", "all_py"]


def input_group_annotation_markers_path(subset: str) -> Path:
    return Path("results") / subset / "annotation_markers" / "group_level_annotation_markers.csv"


def output_figure_dir(subset: str) -> Path:
    return Path("results") / subset / "annotation_markers" / "figures"


def read_group_annotation_markers(subset: str) -> pd.DataFrame:
    path = input_group_annotation_markers_path(subset)
    if not path.exists():
        raise FileNotFoundError(f"Missing group annotation markers CSV: {path}")
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


def make_annotation_marker_rate_double_bar(group_df: pd.DataFrame, subset: str) -> Path:
    """
    Grouped/double bar chart:
    x = annotation marker
    y = rate
    color = era

    Each bar is annotated with:
        <blocks_with_marker>/<total_blocks>
    """

    df = group_df.copy()

    era_order = ["old_2000s", "new_2020s"]
    label_map = {
        "old_2000s": "2000s",
        "new_2020s": "2020s",
    }
    color_map = _era_color_map()

    df = df.set_index("group").reindex(era_order).reset_index()

    markers = [
        ("todo_block_ratio", "todo_blocks", "TODO"),
        ("fixme_block_ratio", "fixme_blocks", "FIXME"),
        ("xxx_block_ratio", "xxx_blocks", "XXX"),
        ("hack_block_ratio", "hack_blocks", "HACK"),
        ("bug_block_ratio", "bug_blocks", "BUG"),
        ("note_block_ratio", "note_blocks", "NOTE"),
        ("annotation_block_ratio", "annotation_blocks", "ANY"),
    ]

    x = list(range(len(markers)))
    width = 0.38

    old_vals = []
    new_vals = []
    old_labels = []
    new_labels = []

    old_total = int(df.loc[df["group"] == "old_2000s", "total_blocks"].iloc[0])
    new_total = int(df.loc[df["group"] == "new_2020s", "total_blocks"].iloc[0])

    for ratio_col, count_col, _label in markers:
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
    x_labels = [label for _r, _c, label in markers]

    plt.figure(figsize=(10, 6))

    bars_old = plt.bar(x_old, old_vals, width=width,
                       color=color_map["old_2000s"],
                       label=label_map["old_2000s"])

    bars_new = plt.bar(x_new, new_vals, width=width,
                       color=color_map["new_2020s"],
                       label=label_map["new_2020s"])

    for bar, label in zip(bars_old, old_labels):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2 - 0.03,
            height,
            label,
            ha="center",
            va="bottom",
            fontsize=9,
            rotation=0,
        )

    for bar, label in zip(bars_new, new_labels):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2 + 0.04,
            height,
            label,
            ha="center",
            va="bottom",
            fontsize=9,
            rotation=0,
        )

    plt.xticks(x, x_labels)
    plt.xlabel("Annotation marker")
    plt.ylabel("Rate (blocks with marker / total blocks)")
    plt.title(f"Annotation marker rates by era ({subset})")
    plt.legend(title="Era")

    out_path = output_figure_dir(subset) / "annotation_marker_rate_double_bar.png"
    save_current_figure(out_path)
    return out_path


def make_annotation_marker_count_double_bar(group_df: pd.DataFrame, subset: str) -> Path:
    """
    Grouped/double bar chart:
    x = annotation marker
    y = raw block count
    color = era

    Uses pooled group-level counts from group_level_annotation_markers.csv.
    """
    df = group_df.copy()

    era_order = ["old_2000s", "new_2020s"]
    label_map = {
        "old_2000s": "2000s",
        "new_2020s": "2020s",
    }
    color_map = _era_color_map()

    df = df.set_index("group").reindex(era_order).reset_index()

    markers = [
        ("todo_blocks", "TODO"),
        ("fixme_blocks", "FIXME"),
        ("xxx_blocks", "XXX"),
        ("hack_blocks", "HACK"),
        ("bug_blocks", "BUG"),
        ("note_blocks", "NOTE"),
        ("annotation_blocks", "ANY"),
    ]

    x = list(range(len(markers)))
    width = 0.38

    old_vals = []
    new_vals = []

    for col, _label in markers:
        old_series = df.loc[df["group"] == "old_2000s", col]
        new_series = df.loc[df["group"] == "new_2020s", col]

        old_vals.append(float(old_series.iloc[0]) if not old_series.empty else 0.0)
        new_vals.append(float(new_series.iloc[0]) if not new_series.empty else 0.0)

    x_old = [i - width / 2 for i in x]
    x_new = [i + width / 2 for i in x]
    x_labels = [label for _col, label in markers]

    plt.figure(figsize=(10, 6))
    plt.bar(x_old, old_vals, width=width, color=color_map["old_2000s"], label=label_map["old_2000s"])
    plt.bar(x_new, new_vals, width=width, color=color_map["new_2020s"], label=label_map["new_2020s"])

    plt.xticks(x, x_labels)
    plt.xlabel("Annotation marker")
    plt.ylabel("Number of comment blocks")
    plt.title(f"Annotation marker counts by era ({subset})")
    plt.legend(title="Era")

    out_path = output_figure_dir(subset) / "annotation_marker_count_double_bar.png"
    save_current_figure(out_path)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Create annotation marker figures for thesis.")
    parser.add_argument("--subset", required=True, choices=SUBSETS)
    args = parser.parse_args()

    subset = args.subset
    group_df = read_group_annotation_markers(subset)

    out1 = make_annotation_marker_rate_double_bar(group_df, subset)
    out2 = make_annotation_marker_count_double_bar(group_df, subset)

    print(f"[figures, subset: {subset}] Wrote → {out1}")
    print(f"[figures, subset: {subset}] Wrote → {out2}")


if __name__ == "__main__":
    main()