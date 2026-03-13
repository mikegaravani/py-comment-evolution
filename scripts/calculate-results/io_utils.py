from __future__ import annotations

from pathlib import Path
import pandas as pd


SUBSETS = ["core", "core_plus_tests", "tests_only", "all_py"]

# --------------- PATHS ---------------
def input_blocks_parquet_path(subset: str) -> Path:
    return Path("data/processed/comment_blocks_enriched") / subset / "comment_blocks_enriched.parquet"

def input_file_index_parquet_path() -> Path:
    return Path("data/processed/file_index") / "file_index.parquet"

def input_docstrings_parquet_path(subset: str) -> Path:
    return Path("data/processed/docstring_data") / subset / "docstrings.parquet"



def output_results_repo_level(subset: str) -> Path:
    return Path("results") / subset / "summary_stats" / "repo_level.csv"

def output_density_file_level(subset: str) -> Path:
    return Path("results") / subset / "density" / "file_level_density.csv"

def output_density_repo_level(subset: str) -> Path:
    return Path("results") / subset / "density" / "repo_level_density.csv"

def output_density_group_level(subset: str) -> Path:
    return Path("results") / subset / "density" / "group_level_density.csv"

def output_structure_repo_level(subset: str) -> Path:
    return Path("results") / subset / "structure" / "repo_level_structure.csv"

def output_structure_group_level(subset: str) -> Path:
    return Path("results") / subset / "structure" / "group_level_structure.csv"

def output_legal_repo_level(subset: str) -> Path:
    return Path("results") / subset / "legal" / "repo_level_legal.csv"

def output_annotation_markers_repo_level(subset: str) -> Path:
    return Path("results") / subset / "annotation_markers" / "repo_level_annotation_markers.csv"

def output_annotation_markers_group_level(subset: str) -> Path:
    return Path("results") / subset / "annotation_markers" / "group_level_annotation_markers.csv"

def output_tooling_directives_repo_level(subset: str) -> Path:
    return Path("results") / subset / "tooling_directives" / "repo_level_tooling_directives.csv"

def output_tooling_directives_group_level(subset: str) -> Path:
    return Path("results") / subset / "tooling_directives" / "group_level_tooling_directives.csv"

def output_linguistic_features_repo_level(subset: str) -> Path:
    return Path("results") / subset / "linguistic_features" / "repo_level_linguistic_features.csv"

def output_linguistic_features_group_level(subset: str) -> Path:
    return Path("results") / subset / "linguistic_features" / "group_level_linguistic_features.csv"

def output_docstrings_repo_level(subset: str) -> Path:
    return Path("results") / subset / "docstrings" / "repo_level_docstrings.csv"

def output_docstrings_group_level(subset: str) -> Path:
    return Path("results") / subset / "docstrings" / "group_level_docstrings.csv"


# --------------- READ FILES ---------------
def read_blocks(subset: str) -> pd.DataFrame:
    in_path = input_blocks_parquet_path(subset)
    if not in_path.exists():
        raise FileNotFoundError(f"Missing input parquet: {in_path}")
    df = pd.read_parquet(in_path)
    return df

def read_file_index() -> pd.DataFrame:
    in_path = input_file_index_parquet_path()
    if not in_path.exists():
        raise FileNotFoundError(f"Missing file index parquet: {in_path}")
    return pd.read_parquet(in_path)

def read_docstrings(subset: str) -> pd.DataFrame:
    in_path = input_docstrings_parquet_path(subset)
    if not in_path.exists():
        raise FileNotFoundError(f"Missing docstrings parquet: {in_path}")
    return pd.read_parquet(in_path)






# --------------- WRITE FILES ---------------
def write_results_repo_level(df_blocks: pd.DataFrame, subset: str) -> Path:
    out_path = output_results_repo_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_blocks.to_csv(out_path, index=False)
    return out_path

def write_density_file_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_density_file_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_density_repo_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_density_repo_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_density_group_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_density_group_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_structure_repo_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_structure_repo_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_structure_group_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_structure_group_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_legal_repo_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_legal_repo_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_annotation_markers_repo_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_annotation_markers_repo_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_annotation_markers_group_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_annotation_markers_group_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_tooling_directives_repo_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_tooling_directives_repo_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_tooling_directives_group_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_tooling_directives_group_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_linguistic_features_repo_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_linguistic_features_repo_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_linguistic_features_group_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_linguistic_features_group_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_docstrings_repo_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_docstrings_repo_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def write_docstrings_group_level(df: pd.DataFrame, subset: str) -> Path:
    out_path = output_docstrings_group_level(subset)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path