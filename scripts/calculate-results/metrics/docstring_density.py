from __future__ import annotations

import pandas as pd


SUBSET_FLAG_MAP = {
    "core": "subset_core",
    "core_plus_tests": "subset_core_plus_tests",
    "tests_only": "subset_tests_only",
    "all_py": "subset_all_py",
}

SCOPE_KINDS = [
    "module",
    "class",
    "function",
]

FILE_GROUP_COLS = [
    "repo",
    "group",
    "release",
    "file_id",
    "path_rel",
]

REPO_GROUP_COLS = [
    "repo",
    "group",
    "release",
]

GROUP_GROUP_COLS = [
    "group",
]


def _filter_file_index_for_subset(file_index_df: pd.DataFrame, subset: str) -> pd.DataFrame:
    if subset not in SUBSET_FLAG_MAP:
        raise ValueError(f"Unknown subset: {subset}")

    subset_flag = SUBSET_FLAG_MAP[subset]
    if subset_flag not in file_index_df.columns:
        raise KeyError(f"Missing subset flag column in file index: {subset_flag}")

    df = file_index_df.copy()

    if "repo" not in df.columns:
        if "name" in df.columns:
            df = df.rename(columns={"name": "repo"})
        else:
            raise KeyError("file_index.parquet must contain either 'repo' or 'name'")

    df = df[df[subset_flag] == True].copy()
    df = df[df["loc_total"] > 0].copy()

    return df


def _aggregate_docstring_counts_by_file(docstrings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns one row per file_id with:
    - total docstring_count
    - docstring scope counts
    """
    if docstrings_df.empty:
        return pd.DataFrame(
            columns=[
                "file_id",
                "docstring_count",
                "module_docstring_count",
                "class_docstring_count",
                "function_docstring_count",
            ]
        )
    
    df = docstrings_df.copy()
    df["scope"] = df["scope"].replace({"async_function": "function"})

    total_counts = (
        docstrings_df.groupby("file_id", as_index=False)
        .agg(docstring_count=("file_id", "size"))
    )

    scope_counts = (
        docstrings_df.groupby(["file_id", "scope"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    for scope in SCOPE_KINDS:
        if scope not in scope_counts.columns:
            scope_counts[scope] = 0

    scope_counts = scope_counts.rename(
        columns={
            "module": "module_docstring_count",
            "class": "class_docstring_count",
            "function": "function_docstring_count",
        }
    )

    out = total_counts.merge(scope_counts, on="file_id", how="left", validate="one_to_one")

    count_cols = [
        "docstring_count",
        "module_docstring_count",
        "class_docstring_count",
        "function_docstring_count",
    ]
    for col in count_cols:
        out[col] = out[col].fillna(0).astype(int)

    return out


def _add_docstring_scope_ratios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    ratio_specs = [
        ("module_docstring_count", "module_docstring_ratio"),
        ("class_docstring_count", "class_docstring_ratio"),
        ("function_docstring_count", "function_docstring_ratio"),
    ]

    for count_col, ratio_col in ratio_specs:
        df[ratio_col] = 0.0
        nonzero_mask = df["docstring_count"] > 0
        df.loc[nonzero_mask, ratio_col] = (
            df.loc[nonzero_mask, count_col] / df.loc[nonzero_mask, "docstring_count"]
        )

    return df


def compute_docstring_density_metrics(
    file_index_df: pd.DataFrame,
    docstrings_df: pd.DataFrame,
    subset: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute docstring density metrics at:
    - repo level
    - group level

    Rules:
    - file_index.parquet is the authority for file inclusion
    - subset membership is determined from file_index subset boolean flags
    - files with zero LOC are excluded
    - files with no docstrings are retained with docstring_count = 0
    """

    files_df = _filter_file_index_for_subset(file_index_df, subset)
    docstring_counts_df = _aggregate_docstring_counts_by_file(docstrings_df)

    docstring_file_ids = set(docstring_counts_df["file_id"].unique()) if not docstring_counts_df.empty else set()
    file_index_file_ids = set(files_df["file_id"].unique())
    orphan_docstring_file_ids = docstring_file_ids - file_index_file_ids

    file_level_df = files_df.merge(
        docstring_counts_df,
        on="file_id",
        how="left",
        validate="one_to_one",
    )

    count_cols = [
        "docstring_count",
        "module_docstring_count",
        "class_docstring_count",
        "function_docstring_count",
    ]
    for col in count_cols:
        file_level_df[col] = file_level_df[col].fillna(0).astype(int)

    file_level_df["docstrings_per_file"] = file_level_df["docstring_count"].astype(float)
    file_level_df["docstrings_per_kloc"] = (
        file_level_df["docstring_count"] / (file_level_df["loc_total"] / 1000.0)
    )

    file_level_df = _add_docstring_scope_ratios(file_level_df)

    repo_level_df = (
        file_level_df.groupby(REPO_GROUP_COLS, as_index=False)
        .agg(
            file_count=("file_id", "count"),
            loc_total=("loc_total", "sum"),
            docstring_count=("docstring_count", "sum"),
            module_docstring_count=("module_docstring_count", "sum"),
            class_docstring_count=("class_docstring_count", "sum"),
            function_docstring_count=("function_docstring_count", "sum"),
        )
    )

    repo_level_df["subset"] = subset
    repo_level_df["docstrings_per_file"] = (
        repo_level_df["docstring_count"] / repo_level_df["file_count"]
    )
    repo_level_df["docstrings_per_kloc"] = (
        repo_level_df["docstring_count"] / (repo_level_df["loc_total"] / 1000.0)
    )
    repo_level_df = _add_docstring_scope_ratios(repo_level_df)

    repo_level_out = repo_level_df[
        [
            "subset",
            "repo",
            "group",
            "release",
            "file_count",
            "loc_total",
            "docstring_count",
            "docstrings_per_file",
            "docstrings_per_kloc",
            "module_docstring_count",
            "class_docstring_count",
            "function_docstring_count",
            "module_docstring_ratio",
            "class_docstring_ratio",
            "function_docstring_ratio",
        ]
    ].copy()

    group_level_df = (
        repo_level_out.groupby(GROUP_GROUP_COLS, as_index=False)
        .agg(
            repo_count=("repo", "count"),
            file_count=("file_count", "sum"),
            loc_total=("loc_total", "sum"),
            docstring_count=("docstring_count", "sum"),
            module_docstring_count=("module_docstring_count", "sum"),
            class_docstring_count=("class_docstring_count", "sum"),
            function_docstring_count=("function_docstring_count", "sum"),
            mean_repo_docstrings_per_file=("docstrings_per_file", "mean"),
            median_repo_docstrings_per_file=("docstrings_per_file", "median"),
            mean_repo_docstrings_per_kloc=("docstrings_per_kloc", "mean"),
            median_repo_docstrings_per_kloc=("docstrings_per_kloc", "median"),
            mean_repo_module_docstring_ratio=("module_docstring_ratio", "mean"),
            median_repo_module_docstring_ratio=("module_docstring_ratio", "median"),
            mean_repo_class_docstring_ratio=("class_docstring_ratio", "mean"),
            median_repo_class_docstring_ratio=("class_docstring_ratio", "median"),
            mean_repo_function_docstring_ratio=("function_docstring_ratio", "mean"),
            median_repo_function_docstring_ratio=("function_docstring_ratio", "median"),
        )
    )

    group_level_df["subset"] = subset
    group_level_df["docstrings_per_file"] = (
        group_level_df["docstring_count"] / group_level_df["file_count"]
    )
    group_level_df["docstrings_per_kloc"] = (
        group_level_df["docstring_count"] / (group_level_df["loc_total"] / 1000.0)
    )
    group_level_df = _add_docstring_scope_ratios(group_level_df)

    group_level_out = group_level_df[
        [
            "subset",
            "group",
            "repo_count",
            "file_count",
            "loc_total",
            "docstring_count",
            "docstrings_per_file",
            "docstrings_per_kloc",
            "module_docstring_count",
            "class_docstring_count",
            "function_docstring_count",
            "module_docstring_ratio",
            "class_docstring_ratio",
            "function_docstring_ratio",
            "mean_repo_docstrings_per_file",
            "median_repo_docstrings_per_file",
            "mean_repo_docstrings_per_kloc",
            "median_repo_docstrings_per_kloc",
            "mean_repo_module_docstring_ratio",
            "median_repo_module_docstring_ratio",
            "mean_repo_class_docstring_ratio",
            "median_repo_class_docstring_ratio",
            "mean_repo_function_docstring_ratio",
            "median_repo_function_docstring_ratio",
        ]
    ].copy()

    if orphan_docstring_file_ids:
        print(
            f"[docstrings, subset: {subset}] Warning: "
            f"{len(orphan_docstring_file_ids)} file_id(s) appear in docstrings "
            f"but not in subset-filtered file_index. They were ignored."
        )

    return repo_level_out, group_level_out