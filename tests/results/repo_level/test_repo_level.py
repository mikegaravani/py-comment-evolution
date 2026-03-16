# python tests/results/repo_level/test_repo_level.py

import pandas as pd
from pathlib import Path

file_path = Path("results/core/summary_stats/repo_level.csv")
out_path = Path("tests/results/repo_level/repo_level_report.txt")


def main(csv_path: str, output_path: str) -> None:
    df = pd.read_csv(csv_path)

    required = {"repo", "group", "total_blocks"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    id_cols = {"subset", "repo", "group", "release", "total_blocks"}
    exclude_cols = {
        "total_comment_lines",
        "mean_block_char_len",
        "median_block_char_len",
        "mean_block_word_len",
        "median_block_word_len",
        "mean_block_lines",
        "median_block_lines",
        "inline_block_ratio",
        "full_line_block_ratio",
        "legal_block_ratio",
        "annotation_block_ratio",
        "tooling_block_ratio",
        "punctuation_end_ratio",
        "question_ratio",
        "exclamation_ratio",
        "url_ratio",
        "separator_ratio",
    }
    metric_cols = [
        c for c in df.columns
        if c not in id_cols
        and c not in exclude_cols
        and pd.api.types.is_numeric_dtype(df[c])
    ]

    groups = sorted(df["group"].dropna().unique())

    with open(output_path, "w") as f:
        for metric in metric_cols:
            print("=" * 100, file=f)
            print(f"METRIC: {metric}", file=f)
            print("=" * 100, file=f)

            df_metric = df.copy()
            df_metric["pct_blocks"] = (df_metric[metric] / df_metric["total_blocks"]) * 100

            # % of blocks with this property per repo
            print("\n% of blocks with this property per repo:", file=f)

            for group_name in groups:
                g = df_metric[df_metric["group"] == group_name].copy()

                print(f"\n  Group: {group_name}", file=f)

                g = g.sort_values("pct_blocks", ascending=False)

                for _, row in g.iterrows():
                    print(
                        f"    {row['repo']:<24}"
                        f"value={row[metric]:>10.2f}   "
                        f"blocks={row['total_blocks']:>6}   "
                        f"pct_blocks={row['pct_blocks']:>8.2f}%",
                        file=f,
                    )

            # 1) Repo-average % (each repo counts equally)
            print("\nGroup average % for this metric (repo-weighted):", file=f)

            repo_avg = df_metric.groupby("group")["pct_blocks"].mean()

            for group_name, pct in repo_avg.sort_values(ascending=False).items():
                print(
                    f"  {group_name:<16} avg_pct_blocks={pct:>8.2f}%",
                    file=f,
                )

            # 2) Block-weighted % (uses total blocks in the group)
            print("\nGroup average % for this metric (block-weighted):", file=f)

            group_sums = df_metric.groupby("group")[[metric, "total_blocks"]].sum()

            block_weighted = (group_sums[metric] / group_sums["total_blocks"]) * 100

            for group_name, pct in block_weighted.sort_values(ascending=False).items():
                print(
                    f"  {group_name:<16} weighted_pct_blocks={pct:>8.2f}%",
                    file=f,
                )

            # 3) Overall percentage across all repos combined
            print("\nOverall % for this metric across all repos:", file=f)

            overall_pct = (df_metric[metric].sum() / df_metric["total_blocks"].sum()) * 100

            print(
                f"  {'ALL_REPOS':<16} overall_pct_blocks={overall_pct:>8.2f}%",
                file=f,
            )

            print(file=f)


if __name__ == "__main__":
    main(file_path, out_path)