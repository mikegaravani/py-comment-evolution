# python tests/results/density/test_density.py

import pandas as pd
from pathlib import Path


file_path = Path("results/core/density/repo_level_density.csv")
out_path = Path("tests/results/density/density_report.txt")


def ascii_bar(value, max_value, width=40, char="#"):
    """Return a proportional ASCII bar."""
    if pd.isna(value) or max_value <= 0:
        return ""
    filled = int(round((value / max_value) * width))
    return char * filled


def fmt_float(value, digits=2):
    return f"{value:.{digits}f}"


def section_title(title, width=88, fill="="):
    return f"\n{title}\n{fill * min(len(title), width)}\n"


def table_line(cols, widths):
    return "  ".join(str(col).ljust(width) for col, width in zip(cols, widths))


def write_metric_chart(lines, df, metric, label, width=40):
    lines.append(f"\n{label}")
    lines.append("-" * len(label))

    max_value = df[metric].max()
    name_width = max(df["repo"].astype(str).map(len).max(), 8)

    for _, row in df.sort_values(metric, ascending=False).iterrows():
        repo = row["repo"]
        value = row[metric]
        bar = ascii_bar(value, max_value, width=width)
        lines.append(
            f"{repo.ljust(name_width)}  {str(row['release']).ljust(10)}  "
            f"{fmt_float(value).rjust(8)}  |{bar}"
        )


def main():
    if not file_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {file_path}")

    df = pd.read_csv(file_path)

    required_columns = {
        "subset",
        "repo",
        "group",
        "release",
        "file_count",
        "loc_total",
        "block_count",
        "blocks_per_file",
        "blocks_per_kloc",
    }
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append("REPO DENSITY REPORT")
    lines.append("===================")
    lines.append(f"Source: {file_path}")
    lines.append(f"Rows: {len(df)}")

    groups = sorted(df["group"].dropna().unique())

    # Overall summary
    lines.append(section_title("Overall summary"))
    summary = (
        df.groupby("group", dropna=False)
        .agg(
            repos=("repo", "count"),
            total_files=("file_count", "sum"),
            total_loc=("loc_total", "sum"),
            total_blocks=("block_count", "sum"),
            avg_blocks_per_file=("blocks_per_file", "mean"),
            avg_blocks_per_kloc=("blocks_per_kloc", "mean"),
        )
        .reset_index()
        .sort_values("group")
    )

    widths = [12, 6, 12, 12, 14, 20, 20]
    header = [
        "group",
        "repos",
        "total_files",
        "total_loc",
        "total_blocks",
        "avg_blocks/file",
        "avg_blocks/kloc",
    ]
    lines.append(table_line(header, widths))
    lines.append(table_line(["-" * w for w in widths], widths))

    for _, row in summary.iterrows():
        lines.append(
            table_line(
                [
                    row["group"],
                    int(row["repos"]),
                    int(row["total_files"]),
                    int(row["total_loc"]),
                    int(row["total_blocks"]),
                    fmt_float(row["avg_blocks_per_file"]),
                    fmt_float(row["avg_blocks_per_kloc"]),
                ],
                widths,
            )
        )

    # Per-group detail sections
    for group in groups:
        gdf = df[df["group"] == group].copy().sort_values("repo")

        lines.append(section_title(f"Group: {group}"))

        lines.append("Repositories")
        lines.append("------------")

        widths = [22, 12, 10, 10, 12, 16, 16]
        header = [
            "repo",
            "release",
            "files",
            "loc",
            "blocks",
            "blocks/file",
            "blocks/kloc",
        ]
        lines.append(table_line(header, widths))
        lines.append(table_line(["-" * w for w in widths], widths))

        for _, row in gdf.iterrows():
            lines.append(
                table_line(
                    [
                        row["repo"],
                        row["release"],
                        int(row["file_count"]),
                        int(row["loc_total"]),
                        int(row["block_count"]),
                        fmt_float(row["blocks_per_file"]),
                        fmt_float(row["blocks_per_kloc"]),
                    ],
                    widths,
                )
            )

        # Group stats
        lines.append("\nGroup stats")
        lines.append("-----------")
        lines.append(f"Repo count        : {len(gdf)}")
        lines.append(f"Total files       : {int(gdf['file_count'].sum())}")
        lines.append(f"Total LOC         : {int(gdf['loc_total'].sum())}")
        lines.append(f"Total blocks      : {int(gdf['block_count'].sum())}")
        lines.append(f"Avg blocks/file   : {fmt_float(gdf['blocks_per_file'].mean())}")
        lines.append(f"Avg blocks/KLOC   : {fmt_float(gdf['blocks_per_kloc'].mean())}")

        # Charts
        write_metric_chart(lines, gdf, "block_count", "Chart: block_count by repo")
        write_metric_chart(lines, gdf, "blocks_per_file", "Chart: blocks_per_file by repo")
        write_metric_chart(lines, gdf, "blocks_per_kloc", "Chart: blocks_per_kloc by repo")

    # Cross-group comparison charts
    lines.append(section_title("Cross-group comparison"))

    group_avg = (
        df.groupby("group", dropna=False)
        .agg(
            avg_blocks_per_file=("blocks_per_file", "mean"),
            avg_blocks_per_kloc=("blocks_per_kloc", "mean"),
            total_blocks=("block_count", "sum"),
        )
        .reset_index()
        .sort_values("group")
    )

    max_total_blocks = group_avg["total_blocks"].max()
    max_avg_bpf = group_avg["avg_blocks_per_file"].max()
    max_avg_bpk = group_avg["avg_blocks_per_kloc"].max()

    lines.append("Total block_count by group")
    lines.append("--------------------------")
    for _, row in group_avg.sort_values("total_blocks", ascending=False).iterrows():
        bar = ascii_bar(row["total_blocks"], max_total_blocks)
        lines.append(f"{row['group'].ljust(12)} {str(int(row['total_blocks'])).rjust(8)}  |{bar}")

    lines.append("\nAverage blocks_per_file by group")
    lines.append("--------------------------------")
    for _, row in group_avg.sort_values("avg_blocks_per_file", ascending=False).iterrows():
        bar = ascii_bar(row["avg_blocks_per_file"], max_avg_bpf)
        lines.append(f"{row['group'].ljust(12)} {fmt_float(row['avg_blocks_per_file']).rjust(8)}  |{bar}")

    lines.append("\nAverage blocks_per_kloc by group")
    lines.append("--------------------------------")
    for _, row in group_avg.sort_values("avg_blocks_per_kloc", ascending=False).iterrows():
        bar = ascii_bar(row["avg_blocks_per_kloc"], max_avg_bpk)
        lines.append(f"{row['group'].ljust(12)} {fmt_float(row['avg_blocks_per_kloc']).rjust(8)}  |{bar}")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote report to: {out_path}")


if __name__ == "__main__":
    main()