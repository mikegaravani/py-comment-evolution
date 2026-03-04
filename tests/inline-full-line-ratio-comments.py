"""
USAGE:
  python tests/inline-full-line-ratio-comments.py
"""

import pandas as pd

from pathlib import Path

parquet_path = Path("data/processed/tokenized_data/core/comments_token.parquet")


def safe_div(numer: float, denom: float) -> float:
    if denom == 0:
        return float("nan")
    return numer / denom


def main() -> int:
    cols = ["repo", "group", "kind"]
    df = pd.read_parquet(parquet_path, columns=cols)

    print(f"Number of comments loaded: {len(df)}")

    df = df[df["group"].isin(["old_2000s", "new_2020s"])].copy()

    df = df[df["kind"].isin(["inline", "full_line"])].copy()

    counts = (
        df.groupby(["group", "repo", "kind"], dropna=False)
          .size()
          .unstack("kind", fill_value=0)
          .reset_index()
    )

    if "inline" not in counts.columns:
        counts["inline"] = 0
    if "full_line" not in counts.columns:
        counts["full_line"] = 0

    counts["ratio_inline_to_full_line"] = counts.apply(
        lambda r: safe_div(float(r["inline"]), float(r["full_line"])),
        axis=1,
    )

    print("\n=== Per-project ratios (inline / full_line) ===")
    out = counts.sort_values(["group", "repo"])[
        ["group", "repo", "inline", "full_line", "ratio_inline_to_full_line"]
    ]
    print(out.to_string(index=False))

    summary = (
        counts.groupby("group")["ratio_inline_to_full_line"]
              .agg(["count", "mean", "median", "std", "min", "max"])
              .reset_index()
    )

    counts["total_comments"] = counts["inline"] + counts["full_line"]
    total_analyzed = int(counts["total_comments"].sum())
    print(f"\nTotal comments analyzed (sum inline + full_line): {total_analyzed}")

    print("\n=== Summary by group ===")
    print(summary.to_string(index=False))

    def get_stat(g: str, col: str) -> float:
        s = summary.loc[summary["group"] == g, col]
        return float(s.iloc[0]) if len(s) else float("nan")

    mean_old = get_stat("old_2000s", "mean")
    mean_new = get_stat("new_2020s", "mean")
    med_old = get_stat("old_2000s", "median")
    med_new = get_stat("new_2020s", "median")

    print("\n=== Comparison (new_2020s vs old_2000s) ===")
    print(f"Mean ratio difference (new - old):   {mean_new - mean_old}")
    print(f"Median ratio difference (new - old): {med_new - med_old}")

    print("\n=== Top 10 projects by ratio in each group ===")
    for g in ["old_2000s", "new_2020s"]:
        top = (
            counts[counts["group"] == g]
            .sort_values("ratio_inline_to_full_line", ascending=False)
            .head(10)[["repo", "inline", "full_line", "ratio_inline_to_full_line"]]
        )
        print(f"\n-- {g} --")
        print(top.to_string(index=False))

    print("\n=== Bottom 10 projects by ratio in each group ===")
    for g in ["old_2000s", "new_2020s"]:
        bottom = (
            counts[counts["group"] == g]
            .sort_values("ratio_inline_to_full_line", ascending=True)
            .head(10)[["repo", "inline", "full_line", "ratio_inline_to_full_line"]]
        )
        print(f"\n-- {g} --")
        print(bottom.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())