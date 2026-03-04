from __future__ import annotations

import hashlib
from typing import Iterable, List, Dict, Any, Tuple
import pandas as pd


FILE_KEY_COLS = ["repo", "group", "release", "file_id", "path_rel"]


def _stable_block_id(
    subset: str,
    repo: str,
    group: str,
    release: str,
    file_id: str,
    path_rel: str,
    block_kind: str,
    start_lineno: int,
    end_lineno: int,
    indent_col: int,
) -> str:
    payload = "\n".join(
        [
            subset,
            repo,
            group,
            release,
            file_id,
            path_rel,
            block_kind,
            str(start_lineno),
            str(end_lineno),
            str(indent_col),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _ensure_required_columns(df: pd.DataFrame) -> None:
    required = set(FILE_KEY_COLS + ["kind", "lineno", "col", "raw_token", "text", "text_stripped", "line_text"])
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input parquet missing required columns: {sorted(missing)}")
    

def _is_shebang_full_line_comment(row: pd.Series) -> bool:
    raw = str(row.get("raw_token", ""))
    txt = str(row.get("text", ""))
    line = str(row.get("line_text", ""))

    # 3 different checks to be extra extra extra safe
    return raw.lstrip().startswith("#!") or line.lstrip().startswith("#!") or txt.lstrip().startswith("#!")


def build_comment_blocks(
    df_comments: pd.DataFrame,
    subset: str,
) -> pd.DataFrame:
    """
    Creates block-level rows.

    Rules:
    - Full-line comments are merged into blocks if consecutive line numbers and same indentation
    - Inline comments are kept as singleton blocks
    - Blocking is done file after file
    - !! Shebangs are treated as a singleton block even if they are attached to other comments
    """

    _ensure_required_columns(df_comments)

    # making sure that columns are sorted
    sort_cols = FILE_KEY_COLS + ["lineno", "col"]
    df = df_comments.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    # one row per block
    out_rows: List[Dict[str, Any]] = []

    # grouping per file
    for file_key, g in df.groupby(FILE_KEY_COLS, sort=False):
        repo, group, release, file_id, path_rel = file_key
        release_date = g["release_date"].iloc[0] if "release_date" in g.columns else None
        directory_id = g["directory_id"].iloc[0] if "directory_id" in g.columns else None
        snapshot_root = g["snapshot_root"].iloc[0] if "snapshot_root" in g.columns else None

        is_full_line = g["kind"].astype(str) == "full_line"
        is_inline = g["kind"].astype(str) == "inline"

        full = g[is_full_line].copy() # full line could be singleton blocks or multi line blocks
        inline = g[is_inline].copy() # inline will be singleton blocks

        # full line comments of file
        if not full.empty:
            # sort full line comments to make sure they are in the right order for merging into blocks
            full = full.sort_values(["lineno", "col"], kind="mergesort").reset_index(drop=True)

            current_members: List[pd.Series] = []
            prev_lineno: int | None = None
            prev_col: int | None = None

            # FUNCTION TO FLUSH CURRENT FINISHED BLOCK
            def flush_current():
                if not current_members:
                    return
                start_lineno = int(current_members[0]["lineno"])
                end_lineno = int(current_members[-1]["lineno"]) # last element

                n_lines = len(current_members)
                is_singleton = n_lines == 1
                block_kind = "full_line_singleton" if is_singleton else "full_line_block"

                indent_col = int(current_members[0]["col"]) # should all be the same so we just take first
                member_linenos = [int(m["lineno"]) for m in current_members]
                member_cols = [int(m["col"]) for m in current_members]
                member_texts = [str(m["text_stripped"]) for m in current_members]
                member_raw_tokens = [str(m["raw_token"]) for m in current_members]
                member_line_texts = [str(m["line_text"]) for m in current_members]

                # extra fields for whole block text
                block_text_raw = "\n".join(member_raw_tokens)
                block_text_stripped = "\n".join(member_texts)

                block_id = _stable_block_id(
                    subset=subset,
                    repo=str(repo),
                    group=str(group),
                    release=str(release),
                    file_id=str(file_id),
                    path_rel=str(path_rel),
                    block_kind=block_kind,
                    start_lineno=start_lineno,
                    end_lineno=end_lineno,
                    indent_col=indent_col,
                )

                out_rows.append(
                    {
                        "subset": subset,
                        "repo": repo,
                        "group": group,
                        "release": release,
                        "release_date": release_date,
                        "directory_id": directory_id,
                        "snapshot_root": snapshot_root,
                        "path_rel": path_rel,
                        "file_id": file_id,
                        "block_id": block_id,
                        "block_kind": block_kind,
                        "start_lineno": start_lineno,
                        "end_lineno": end_lineno,
                        "indent_col": indent_col,
                        "n_lines": len(current_members),
                        "is_singleton": is_singleton,
                        "member_linenos": member_linenos,
                        "member_cols": member_cols,
                        "member_texts": member_texts,
                        "member_raw_tokens": member_raw_tokens,
                        "member_line_texts": member_line_texts,
                        "block_text_raw": block_text_raw,
                        "block_text_stripped": block_text_stripped,
                        "block_char_len": len(block_text_stripped),
                        "block_word_len": len(block_text_stripped.split()),
                    }
                )

            # iter rows of full line comments of a file
            for _, row in full.iterrows():
                lineno = int(row["lineno"])
                col = int(row["col"])

                if lineno == 1 and _is_shebang_full_line_comment(row):
                    flush_current() # there shouldn't be any
                    current_members = [row]
                    prev_lineno, prev_col = lineno, col
                    flush_current()
                    current_members = []
                    prev_lineno, prev_col = None, None
                    continue

                if not current_members:
                    current_members = [row]
                    prev_lineno, prev_col = lineno, col
                    continue

                is_adjacent = (prev_lineno is not None) and (lineno == prev_lineno + 1)
                same_indent_ok = (prev_col is not None) and (col == prev_col)

                if is_adjacent and same_indent_ok:
                    # we found another comment to put in block
                    current_members.append(row)
                    prev_lineno, prev_col = lineno, col
                else:
                    # flush current block and start new one
                    flush_current()
                    current_members = [row]
                    prev_lineno, prev_col = lineno, col

            # file finished, flush any remaining block
            flush_current()






        # inline comments of file
        if not inline.empty:
            inline = inline.sort_values(["lineno", "col"], kind="mergesort").reset_index(drop=True)
            for _, row in inline.iterrows():
                lineno = int(row["lineno"])
                col = int(row["col"])
                text = str(row["text_stripped"])
                raw_token = str(row["raw_token"])
                line_text = str(row["line_text"])
                block_kind = "inline"

                block_id = _stable_block_id(
                    subset=subset,
                    repo=str(repo),
                    group=str(group),
                    release=str(release),
                    file_id=str(file_id),
                    path_rel=str(path_rel),
                    block_kind=block_kind,
                    start_lineno=lineno,
                    end_lineno=lineno,
                    indent_col=col,
                )

                out_rows.append(
                    {
                        "subset": subset,
                        "repo": repo,
                        "group": group,
                        "release": release,
                        "release_date": release_date,
                        "directory_id": directory_id,
                        "snapshot_root": snapshot_root,
                        "path_rel": path_rel,
                        "file_id": file_id,
                        "block_id": block_id,
                        "block_kind": block_kind,
                        "start_lineno": lineno,
                        "end_lineno": lineno,
                        "indent_col": col,
                        "n_lines": 1,
                        "is_singleton": True,
                        "member_linenos": [lineno],
                        "member_cols": [col],
                        "member_texts": [text],
                        "member_raw_tokens": [raw_token],
                        "member_line_texts": [line_text],
                        "block_text_raw": raw_token,
                        "block_text_stripped": text,
                        "block_char_len": len(text),
                        "block_word_len": len(text.split()),
                    }
                )

    df_blocks = pd.DataFrame(out_rows)
    
    if not df_blocks.empty:
        df_blocks = df_blocks.sort_values(
            ["repo", "path_rel", "start_lineno", "indent_col", "block_kind"],
            kind="mergesort",
        ).reset_index(drop=True)

    return df_blocks