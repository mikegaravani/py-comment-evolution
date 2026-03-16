"""
Create quick tooling directive figures for the thesis.

USAGE:
    python scripts/visualize-results/visualize-td.py --subset core
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


SUBSETS = ["core", "core_plus_tests", "tests_only", "all_py"]


def input_group_tooling_directives_path(subset: str) -> Path:
    return Path("results") / subset / "tooling_directives" / "group_level_tooling_directives.csv"


def output_figure_dir(subset: str) -> Path:
    return Path("results") / subset / "tooling_directives" / "figures"


def read_group_tooling_directives(subset: str) -> pd.DataFrame:
    path = input_group_tooling_directives_path(subset)
    if not path.exists():
        raise FileNotFoundError(f"Missing group tooling directives CSV: {path}")
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


def make_tooling_directive_rate_double_bar(group_df: pd.DataFrame, subset: str) -> Path:
    """
    Grouped/double bar chart:
    x = tooling directive
    y = rate
    color = era

    Each bar is annotated with:
        <blocks_with_directive>/<total_blocks>
    """

    df = group_df.copy()

    era_order = ["old_2000s", "new_2020s"]
    label_map = {
        "old_2000s": "2000s",
        "new_2020s": "2020s",
    }
    color_map = _era_color_map()

    df = df.set_index("group").reindex(era_order).reset_index()

    directives = [
        ("noqa_block_ratio", "noqa_blocks", "NOQA"),
        ("pragma_block_ratio", "pragma_blocks", "PRAGMA"),
        ("type_ignore_block_ratio", "type_ignore_blocks", "TYPE_IGNORE"),
        ("pylint_block_ratio", "pylint_blocks", "PYLINT"),
        ("mypy_block_ratio", "mypy_blocks", "MYPY"),
        ("fmt_block_ratio", "fmt_blocks", "FMT"),
        ("encoding_block_ratio", "encoding_blocks", "ENCODING"),
        ("shebang_block_ratio", "shebang_blocks", "SHEBANG"),
        ("tooling_block_ratio", "tooling_blocks", "ANY"),
    ]

    x = list(range(len(directives)))
    width = 0.38

    old_vals = []
    new_vals = []
    old_labels = []
    new_labels = []

    old_total = int(df.loc[df["group"] == "old_2000s", "total_blocks"].iloc[0])
    new_total = int(df.loc[df["group"] == "new_2020s", "total_blocks"].iloc[0])

    for ratio_col, count_col, _label in directives:
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
    x_labels = [label for _r, _c, label in directives]

    plt.figure(figsize=(11, 6))

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

    for bar, label in zip(bars_old, old_labels):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2 - 0.03,
            height + 0.002,
            label,
            ha="center",
            va="bottom",
            fontsize=9,
            rotation=0,
        )

    for bar, label in zip(bars_new, new_labels):
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2 + 0.03,
            height + 0.002,
            label,
            ha="center",
            va="bottom",
            fontsize=9,
            rotation=0,
        )

    plt.xticks(x, x_labels)
    plt.xlabel("Tooling directive")
    plt.ylabel("Rate (blocks with directive / total blocks)")
    plt.title(f"Tooling directive rates by era ({subset})")
    plt.legend(title="Era")

    out_path = output_figure_dir(subset) / "tooling_directive_rate_double_bar.png"
    save_current_figure(out_path)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Create tooling directive figures for thesis.")
    parser.add_argument("--subset", required=True, choices=SUBSETS)
    args = parser.parse_args()

    subset = args.subset
    group_df = read_group_tooling_directives(subset)

    out_path = make_tooling_directive_rate_double_bar(group_df, subset)
    print(f"[figures, subset: {subset}] Wrote → {out_path}")


if __name__ == "__main__":
    main()