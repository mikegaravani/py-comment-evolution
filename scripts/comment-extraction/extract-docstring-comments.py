"""
Docstring extraction using AST (fast) with Parso fallback for old Python code (for example Python 2).

USAGE:
    # core python only
    python scripts/comment-extraction/extract-docstring-comments.py --subset core --write-file-status

    # core + tests
    python scripts/comment-extraction/extract-docstring-comments.py --subset core_plus_tests --write-file-status

    # tests only
    python scripts/comment-extraction/extract-docstring-comments.py --subset tests_only --write-file-status

    # all python
    python scripts/comment-extraction/extract-docstring-comments.py --subset all_py --write-file-status
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _lib.io import load_file_index, read_text_file, row_to_file_row
from _lib.docstring_runner import iter_docstrings


SUBSET_TO_COLUMN = {
    "core": "subset_core",
    "core_plus_tests": "subset_core_plus_tests",
    "tests_only": "subset_tests_only",
    "all_py": "subset_all_py",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract docstrings from python files in file_index.parquet (AST + Parso fallback)."
    )
    parser.add_argument(
        "--file-index",
        default="data/processed/file_index/file_index.parquet",
        help="Path to canonical file_index.parquet",
    )
    parser.add_argument(
        "--subset",
        choices=list(SUBSET_TO_COLUMN.keys()),
        default="core",
        help="Which subset flag to use from file_index",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output parquet path. Default: data/processed/tokenized_data/<subset>/docstrings.parquet",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit of files to process (debug)",
    )
    parser.add_argument(
        "--write-file-status",
        action="store_true",
        help="Also write per-file status parquet (parse ok/read errors counts).",
    )
    args = parser.parse_args()

    subset_col = SUBSET_TO_COLUMN[args.subset]

    out_path = args.out
    if out_path is None:
        out_dir = Path("data/processed/docstring_data") / args.subset
        out_path = out_dir / "docstrings.parquet"
    else:
        out_path = Path(out_path)
        out_dir = out_path.parent

    out_dir.mkdir(parents=True, exist_ok=True)

    df_index = load_file_index(args.file_index)
    df_sel = df_index[df_index[subset_col]].copy()
    if args.limit is not None:
        df_sel = df_sel.head(args.limit)

    doc_rows: list[dict] = []
    file_status_rows: list[dict] = []

    for _, r in df_sel.iterrows():
        fr = row_to_file_row(r)

        text, read_err = read_text_file(fr.snapshot_root, fr.path_rel)
        if text is None:
            file_status_rows.append(
                {
                    "file_id": fr.file_id,
                    "repo": fr.name,
                    "group": fr.group,
                    "release": fr.release,
                    "path_rel": fr.path_rel,
                    "subset": args.subset,
                    "read_ok": False,
                    "read_error": read_err,
                    "parse_ok": False,
                    "parse_backend": None,
                    "parse_version": None,
                    "parse_error": None,
                    "n_docstrings": 0,
                }
            )
            continue

        n_docstrings = 0
        parse_ok = True
        parse_backend = None
        parse_version = None
        parse_error = None

        try:
            for drow in iter_docstrings(
                source_text=text,
                file_id=fr.file_id,
                repo=fr.name,
                group=fr.group,
                release=fr.release,
                release_date=fr.release_date,
                directory_id=fr.directory_id,
                snapshot_root=fr.snapshot_root,
                path_rel=fr.path_rel,
            ):
                # backend/version will be consistent within a file (ast or parso)
                parse_backend = drow.parse_backend
                parse_version = drow.parse_version

                doc_rows.append(drow.__dict__)
                n_docstrings += 1

            # If there were no docstrings, iter_docstrings may still have parsed successfully.
            # parse_backend might still be None; treat as AST success (default attempt).
            if parse_backend is None:
                parse_backend = "ast"
                parse_version = None

        except Exception as e:
            parse_ok = False
            parse_error = f"{type(e).__name__}"

            file_status_rows.append(
                {
                    "file_id": fr.file_id,
                    "repo": fr.name,
                    "group": fr.group,
                    "release": fr.release,
                    "path_rel": fr.path_rel,
                    "subset": args.subset,
                    "read_ok": True,
                    "read_error": read_err,
                    "parse_ok": False,
                    "parse_backend": parse_backend,
                    "parse_version": parse_version,
                    "parse_error": parse_error,
                    "n_docstrings": n_docstrings,
                }
            )
            continue

        file_status_rows.append(
            {
                "file_id": fr.file_id,
                "repo": fr.name,
                "group": fr.group,
                "release": fr.release,
                "path_rel": fr.path_rel,
                "subset": args.subset,
                "read_ok": True,
                "read_error": read_err,
                "parse_ok": parse_ok,
                "parse_backend": parse_backend,
                "parse_version": parse_version,
                "parse_error": parse_error,
                "n_docstrings": n_docstrings,
            }
        )

    df_docs = pd.DataFrame(doc_rows)
    df_docs.to_parquet(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(f"Rows (docstrings): {len(df_docs):,}")
    if len(df_docs) > 0:
        print("\nQuick sanity:")
        print(df_docs["scope"].value_counts().to_string())
        print("\nBackend usage:")
        print(df_docs["parse_backend"].value_counts().to_string())

    if args.write_file_status:
        status_path = out_dir / "file_status_docstrings.parquet"
        df_status = pd.DataFrame(file_status_rows)
        df_status.to_parquet(status_path, index=False)
        print(f"Wrote: {status_path}")

        summary_path = out_dir / "file_status_docstrings_summary.csv"
        df_summary = (
            df_status.groupby(["repo", "group", "subset"], as_index=False)
            .agg(
                n_files=("file_id", "count"),
                n_read_ok=("read_ok", "sum"),
                n_parse_ok=("parse_ok", "sum"),
                n_docstrings=("n_docstrings", "sum"),
            )
        )
        df_summary.to_csv(summary_path, index=False)
        print(f"Wrote: {summary_path}")


if __name__ == "__main__":
    main()