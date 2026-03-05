from __future__ import annotations

import re
import pandas as pd


_SPDX_RE = re.compile(r"\bSPDX", re.IGNORECASE)
_COPYRIGHT_RE = re.compile(r"\bcopyright\b|\(c\)|©", re.IGNORECASE)

_LICENSE_WORD_RE = re.compile(r"\blicen[cs]e(?:d)?\b", re.IGNORECASE)
_ALL_RIGHTS_RESERVED_RE = re.compile(r"\ball rights reserved\b", re.IGNORECASE)

_PERMISSION_GRANTED_RE = re.compile(r"\bpermission\s+is\s+hereby\s+granted\b", re.IGNORECASE)
_REDIS_AND_USE_RE = re.compile(r"\bredistribution\s+and\s+use\b", re.IGNORECASE)

_WARRANTY_DISCLAIMER_RE = re.compile(
    r"\b(?:as is|without warranty|warrant(?:y|ies)|liabilit(?:y|ies)|damages)\b",
    re.IGNORECASE,
)

_LICENSE_FAMILY_RE = re.compile(
    r"\b(?:MIT|Apache(?:\s+License)?|BSD|GPL|LGPL|MPL|ISC|EPL|AGPL)\b",
    re.IGNORECASE,
)


def add_legal_header_features(
    df_blocks: pd.DataFrame,
    text_col: str = "block_text_stripped",
) -> pd.DataFrame:
    """
    Detects legal/license/copyright header signals.
    PREFIX FOR VARIABLES ADDED: lh = legal header

    Columns added:
      - lh_has_spdx (bool)
      - lh_has_copyright (bool)
      - lh_has_license_word (bool)
      - lh_has_all_rights_reserved (bool)
      - lh_has_permission_granted (bool)
      - lh_has_redistribution_and_use (bool)
      - lh_has_warranty_disclaimer (bool)
      - lh_has_license_family_mention (bool)

      - lh_has_legal_signal (bool)  [if any of the above content signals is true]
      - lh_number_of_unique_signals (int)
      - lh_kinds (list[str])
    """
    if text_col not in df_blocks.columns:
        raise ValueError(f"Missing expected text column: {text_col}")

    s = df_blocks[text_col].fillna("").astype(str)

    signals = {
        "spdx": _SPDX_RE,
        "copyright": _COPYRIGHT_RE,
        "license_word": _LICENSE_WORD_RE,
        "all_rights_reserved": _ALL_RIGHTS_RESERVED_RE,
        "permission_granted": _PERMISSION_GRANTED_RE,
        "redistribution_and_use": _REDIS_AND_USE_RE,
        "warranty_disclaimer": _WARRANTY_DISCLAIMER_RE,
        "license_family_mention": _LICENSE_FAMILY_RE,
    }

    df_matches = pd.DataFrame({name: s.str.contains(pattern) for name, pattern in signals.items()})

    has_any = df_matches.any(axis=1)
    number_of_unique_signals = df_matches.sum(axis=1).astype(int)

    kinds = df_matches.apply(lambda row: [name for name, present in row.items() if present], axis=1)

    out = df_blocks.copy()
    for name in signals:
        out[f"lh_has_{name}"] = df_matches[name]

    out["lh_has_legal_signal"] = has_any
    out["lh_number_of_unique_signals"] = number_of_unique_signals
    out["lh_kinds"] = kinds

    return out