from __future__ import annotations

import re
import pandas as pd

_PRAGMA_RE = re.compile(r"\bpragma\s*:\s*", re.IGNORECASE)
_NOQA_RE = re.compile(r"\bnoqa\b", re.IGNORECASE)
_TYPE_IGNORE_RE = re.compile(r"\btype\s*:\s*ignore\b", re.IGNORECASE)

_PYLINT_RE = re.compile(r"\bpylint\s*:\s*", re.IGNORECASE)
_MYPY_RE = re.compile(r"\bmypy\s*:\s*", re.IGNORECASE)
_FMT_RE = re.compile(r"\bfmt\s*:\s*", re.IGNORECASE)

_ENCODING_RE = re.compile(r"^\s*-\*-.*?-\*-\s*$", re.IGNORECASE) # finds comments that start and end with -*-


def add_tooling_directive_features(
    df_blocks: pd.DataFrame,
    text_col: str = "block_text_stripped", # without the #
) -> pd.DataFrame:
    """
    Detects comments used for tooling directives (linters, type checkers, coverage tools).

    PREFIX FOR VARIABLES ADDED: td = tooling directive

    Columns added:
      - td_has_pragma (bool)
      - td_has_noqa (bool)
      - td_has_type_ignore (bool)
      - td_has_pylint (bool)
      - td_has_mypy (bool)
      - td_has_fmt (bool)

      - td_has_encoding (bool)

      - td_has_tooling_directive (bool)
      - td_number_of_unique_directives (int)
      - td_kinds (list[str])
    """

    if text_col not in df_blocks.columns:
        raise ValueError(f"Missing expected text column: {text_col}")

    s = df_blocks[text_col].fillna("").astype(str)

    directives = {
        "pragma": _PRAGMA_RE,
        "noqa": _NOQA_RE,
        "type_ignore": _TYPE_IGNORE_RE,
        "pylint": _PYLINT_RE,
        "mypy": _MYPY_RE,
        "fmt": _FMT_RE,
        "encoding": _ENCODING_RE,
    }

    df_matches = pd.DataFrame(
        {name: s.str.contains(pattern) for name, pattern in directives.items()}
    )

    has_any = df_matches.any(axis=1)
    number_of_unique_directives = df_matches.sum(axis=1).astype(int)

    kinds = df_matches.apply(
        lambda row: [name for name, present in row.items() if present], axis=1
    )

    out = df_blocks.copy()

    for name in directives:
        out[f"td_has_{name}"] = df_matches[name]

    out["td_has_tooling_directive"] = has_any
    out["td_number_of_unique_directives"] = number_of_unique_directives
    out["td_kinds"] = kinds

    return out