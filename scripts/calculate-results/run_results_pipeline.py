'''
MAIN PIPELINE FOR PRODUCING RESULTS

USAGE:
    python scripts/calculate-results/run_results_pipeline.py --subset core
    python scripts/calculate-results/run_results_pipeline.py --all
'''
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from metrics.repo_level import compute_repo_level_metrics

from io_utils import SUBSETS, read_blocks, write_results_repo_level


def run_subset(subset: str) -> None:

    print(f"[results] Processing subset: {subset}")

    df = read_blocks(subset)
    repo_metrics = compute_repo_level_metrics(df)
    repo_level_metrics_path = write_results_repo_level(repo_metrics, subset)
    print(f"[results, subset: {subset}] Wrote repo-level metrics → {repo_level_metrics_path}")


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