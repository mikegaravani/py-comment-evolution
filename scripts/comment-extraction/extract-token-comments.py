'''
Token extraction using Python's tokenize module.

USAGE:
    # core python only
    python scripts/comment-extraction/extract_token_comments.py --subset core --write-file-status

    # core + tests
    python scripts/comment-extraction/extract_token_comments.py --subset core_plus_tests --write-file-status

    # tests only
    python scripts/comment-extraction/extract_token_comments.py --subset tests_only --write-file-status

    # all python
    python scripts/comment-extraction/extract_token_comments.py --subset all_py --write-file-status
'''

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _lib.io import load_file_index, read_text_file, row_to_file_row
from _lib.tokenize_runner import iter_token_comments


SUBSET_TO_COLUMN = {
    "core": "subset_core",
    "core_plus_tests": "subset_core_plus_tests",
    "tests_only": "subset_tests_only",
    "all_py": "subset_all_py",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract '#' comments using Python tokenize from python files in file_index.parquet."
    )
    # Default is fine
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
        help="Output parquet path. Default: data/processed/tokenized_data/comments_token_<subset>.parquet",
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
        help="Also write per-file status parquet (tokenize ok/read errors counts).",
    )
    args = parser.parse_args()

    subset_col = SUBSET_TO_COLUMN[args.subset]
    out_path = args.out
    if out_path is None:
        out_path = f"data/processed/tokenized_data/comments_token_{args.subset}.parquet"

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df_index = load_file_index(args.file_index)

    df_sel = df_index[df_index[subset_col]].copy()
    if args.limit is not None:
        df_sel = df_sel.head(args.limit)

    comment_rows = []
    file_status_rows = []

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
                    "tokenize_ok": False,
                    "n_comments": 0,
                }
            )
            continue

        n_comments = 0
        tokenize_ok = True

        try:
            for crow in iter_token_comments(
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
                comment_rows.append(crow.__dict__)
                n_comments += 1
        except Exception as e:
            tokenize_ok = False
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
                    "tokenize_ok": False,
                    "tokenize_error": f"{type(e).__name__}",
                    "n_comments": n_comments,
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
                "tokenize_ok": tokenize_ok,
                "n_comments": n_comments,
            }
        )

    df_comments = pd.DataFrame(comment_rows)
    df_comments.to_parquet(out_path, index=False)

    print(f"Wrote: {out_path}")
    print(f"Rows (comments): {len(df_comments):,}")
    if len(df_comments) > 0:
        print("\nQuick sanity:")
        print(df_comments["kind"].value_counts().to_string())
        print("\nTop pragmatic kinds:")
        if "pragmatic_kinds" in df_comments.columns:
            print(df_comments[df_comments["is_pragmatic"]]["pragmatic_kinds"].value_counts().head(10).to_string())

    if args.write_file_status:
        status_path = out_path.parent / f"file_status_token_{args.subset}.parquet"
        df_status = pd.DataFrame(file_status_rows)
        df_status.to_parquet(status_path, index=False)
        print(f"Wrote: {status_path}")
        summary_path = out_path.parent / f"file_status_token_{args.subset}_summary.csv"
        df_summary = (
            df_status.groupby(["repo", "group", "subset"], as_index=False)
            .agg(
                n_files=("file_id", "count"),
                n_read_ok=("read_ok", "sum"),
                n_tokenize_ok=("tokenize_ok", "sum"),
                n_comments=("n_comments", "sum"),
            )
        )
        df_summary.to_csv(summary_path, index=False)
        print(f"Wrote: {summary_path}")


if __name__ == "__main__":
    main()