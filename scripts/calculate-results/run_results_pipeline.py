'''
MAIN PIPELINE FOR PRODUCING RESULTS

USAGE:
    python scripts/calculate-results/run_results_pipeline.py --subset core
    python scripts/calculate-results/run_results_pipeline.py --all
'''
from __future__ import annotations

import argparse

from metrics.repo_level import compute_repo_level_metrics
from metrics.density import compute_density_metrics
from metrics.structure import compute_structure_metrics
from metrics.legal import compute_legal_metrics
from metrics.annotation_markers import compute_annotation_marker_metrics
from metrics.tooling_directives import compute_tooling_directive_metrics

from io_utils import (
    SUBSETS,
    read_blocks,
    read_file_index,

    write_results_repo_level,

    write_density_file_level,
    write_density_repo_level,
    write_density_group_level,

    write_structure_repo_level,
    write_structure_group_level,

    write_legal_repo_level,

    write_annotation_markers_repo_level,
    write_annotation_markers_group_level,

    write_tooling_directives_repo_level,
    write_tooling_directives_group_level,
)


def run_subset(subset: str) -> None:

    print(f"[results] Processing subset: {subset}")

    blocks_df = read_blocks(subset)
    file_index_df = read_file_index()

    # repo metrics
    repo_metrics_df = compute_repo_level_metrics(blocks_df)
    repo_level_metrics_path = write_results_repo_level(repo_metrics_df, subset)
    print(f"[results, subset: {subset}] Wrote repo-level metrics → {repo_level_metrics_path}")


    # density metrics
    file_density_df, repo_density_df, group_density_df = compute_density_metrics(
        file_index_df=file_index_df,
        blocks_df=blocks_df,
        subset=subset,
    )

    file_density_path = write_density_file_level(file_density_df, subset)
    repo_density_path = write_density_repo_level(repo_density_df, subset)
    group_density_path = write_density_group_level(group_density_df, subset)

    print(f"[results, subset: {subset}] Wrote file-level density → {file_density_path}")
    print(f"[results, subset: {subset}] Wrote repo-level density → {repo_density_path}")
    print(f"[results, subset: {subset}] Wrote group-level density → {group_density_path}")

    # Structure metrics
    repo_structure_df, group_structure_df = compute_structure_metrics(
        blocks_df=blocks_df,
        subset=subset,
    )

    repo_structure_path = write_structure_repo_level(repo_structure_df, subset)
    group_structure_path = write_structure_group_level(group_structure_df, subset)

    print(f"[results, subset: {subset}] Wrote repo-level structure → {repo_structure_path}")
    print(f"[results, subset: {subset}] Wrote group-level structure → {group_structure_path}")

    # Legal metrics
    legal_repo_df = compute_legal_metrics(
        blocks_df=blocks_df,
        subset=subset,
    )

    legal_repo_path = write_legal_repo_level(legal_repo_df, subset)
    print(f"[results, subset: {subset}] Wrote repo-level legal → {legal_repo_path}")

    # Annotation marker metrics
    annotation_repo_df, annotation_group_df = compute_annotation_marker_metrics(
        blocks_df=blocks_df,
        subset=subset,
    )

    annotation_repo_path = write_annotation_markers_repo_level(annotation_repo_df, subset)
    annotation_group_path = write_annotation_markers_group_level(annotation_group_df, subset)

    print(f"[results, subset: {subset}] Wrote repo-level annotation markers → {annotation_repo_path}")
    print(f"[results, subset: {subset}] Wrote group-level annotation markers → {annotation_group_path}")

    # Tooling directive metrics
    tooling_repo_df, tooling_group_df = compute_tooling_directive_metrics(
        blocks_df=blocks_df,
        subset=subset,
    )

    tooling_repo_path = write_tooling_directives_repo_level(tooling_repo_df, subset)
    tooling_group_path = write_tooling_directives_group_level(tooling_group_df, subset)

    print(f"[results, subset: {subset}] Wrote repo-level tooling directives → {tooling_repo_path}")
    print(f"[results, subset: {subset}] Wrote group-level tooling directives → {tooling_group_path}")


def main():

    parser = argparse.ArgumentParser(description="Compute thesis results.")
    parser.add_argument("--subset", choices=SUBSETS)
    parser.add_argument("--all", action="store_true")

    args = parser.parse_args()

    if args.all:
        for subset in SUBSETS:
            run_subset(subset)

    else:
        run_subset(args.subset)


if __name__ == "__main__":
    main()