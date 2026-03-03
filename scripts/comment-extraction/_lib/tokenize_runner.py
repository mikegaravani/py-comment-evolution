from __future__ import annotations

import io
import token
import tokenize
from dataclasses import dataclass
from typing import Iterable

# from .heuristics import extract_features


@dataclass(frozen=True)
class CommentRow:
    file_id: str
    repo: str
    group: str
    release: str
    release_date: str
    directory_id: str
    snapshot_root: str
    path_rel: str

    kind: str  # "inline" or "full_line"
    lineno: int
    col: int

    # raw + normalized
    raw_token: str        # includes '#...'
    text: str             # stripped of leading '#', left-trimmed
    text_stripped: str    # full strip

    # context
    line_text: str        # full line content
    code_prefix: str      # portion before '#'
    prefix_has_code: bool

    char_len: int
    word_len: int


def _classify_inline(line_text: str, comment_col: int) -> tuple[bool, str]:
    """
    Determine whether the comment is inline vs full-line by checking
    if there is any non-whitespace before the comment start column.
    Returns (prefix_has_code, prefix_text).
    """
    if comment_col <= 0:
        prefix = ""
    else:
        prefix = line_text[:comment_col]
    prefix_has_code = bool(prefix.strip())
    return prefix_has_code, prefix


def iter_token_comments(
    *,
    source_text: str,
    file_id: str,
    repo: str,
    group: str,
    release: str,
    release_date: str,
    directory_id: str,
    snapshot_root: str,
    path_rel: str,
) -> Iterable[CommentRow]:
    """
    CommentRow entries for every '#' comment token using Python tokenize.
    This is ONLY for tokenized comments (no docstrings) and avoids false positives inside strings.
    """
    sio = io.StringIO(source_text)
    try:
        tokens = tokenize.generate_tokens(sio.readline)
        for tok in tokens:
            if tok.type != token.COMMENT:
                continue

            raw = tok.string  # includes leading '#'
            lineno, col = tok.start
            # tok.line is the physical line as read by tokenize
            line_text = tok.line.rstrip("\n") if tok.line is not None else ""

            # Normalize comment text
            # Keep raw_token for traceability; text removes only the first leading '#'
            text = raw[1:] if raw.startswith("#") else raw
            text = text.lstrip("\t ").rstrip("\n")
            text_stripped = text.strip()

            prefix_has_code, prefix = _classify_inline(line_text, col)
            kind = "inline" if prefix_has_code else "full_line"

            # Lengths
            char_len = len(text_stripped)
            word_len = len(text_stripped.split()) if text_stripped else 0

            yield CommentRow(
                file_id=file_id,
                repo=repo,
                group=group,
                release=release,
                release_date=release_date,
                directory_id=directory_id,
                snapshot_root=snapshot_root,
                path_rel=path_rel,
                kind=kind,
                lineno=int(lineno),
                col=int(col),
                raw_token=raw,
                text=text,
                text_stripped=text_stripped,
                line_text=line_text,
                code_prefix=prefix,
                prefix_has_code=prefix_has_code,

                char_len=char_len,
                word_len=word_len,
            )
    except tokenize.TokenError as e:
        print(f"[TokenError] {file_id} ({path_rel}): {e}")
        return