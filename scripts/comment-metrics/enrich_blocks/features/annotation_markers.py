from __future__ import annotations

import re
import pandas as pd


_TODO_RE = re.compile(r"\bTODO\b", re.IGNORECASE)
_FIXME_RE = re.compile(r"\bFIXME\b", re.IGNORECASE)
_XXX_RE = re.compile(r"\bXXX\b", re.IGNORECASE)
_HACK_RE = re.compile(r"\bHACK\b", re.IGNORECASE)
_BUG_RE = re.compile(r"\b(?:BUG|BUGFIX)\b", re.IGNORECASE)
_NOTE_RE = re.compile(r"\bNOTE:\s*", re.IGNORECASE) # being stricter with this one because "note" is a common word


def add_annotation_marker_features(df_blocks: pd.DataFrame, text_col: str = "block_text_stripped") -> pd.DataFrame:
    """
    Checks for any annotation markers that could be present in the comment.
    PREFIX FOR VARIABLES ADDED: am = annotation marker

    Columns added:
      - am_has_todo (bool)
      - am_has_fixme (bool)
      - am_has_xxx (bool)
      - am_has_hack (bool)
      - am_has_bug (bool)
      - am_has_note (bool)

      - am_has_annotation_marker (if any of the above is true)
      - am_number_of_unique_markers (int)
      - am_kinds (list[str])
    """
    # extra check
    if text_col not in df_blocks.columns:
        raise ValueError(f"Missing expected text column: {text_col}")

    s = df_blocks[text_col].fillna("").astype(str)

    markers = {
        "todo": _TODO_RE,
        "fixme": _FIXME_RE,
        "xxx": _XXX_RE,
        "hack": _HACK_RE,
        "bug": _BUG_RE,
        "note": _NOTE_RE,
    }

    df_matches = pd.DataFrame({name: s.str.contains(pattern) for name, pattern in markers.items()})

    has_any = df_matches.any(axis=1)
    number_of_unique_markers = df_matches.sum(axis=1).astype(int)

    kinds = df_matches.apply(lambda row: [name for name, present in row.items() if present], axis=1)

    out = df_blocks.copy()
    for name in markers:
        out[f"am_has_{name}"] = df_matches[name]

    out["am_has_annotation_marker"] = has_any
    out["am_number_of_unique_markers"] = number_of_unique_markers
    out["am_kinds"] = kinds

    return out