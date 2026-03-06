from __future__ import annotations

import re
import pandas as pd


# punctuation endings
_ENDS_WITH_PERIOD_RE = re.compile(r"\.\s*$")
_ENDS_WITH_QMARK_RE = re.compile(r"\?\s*$")
_ENDS_WITH_EMARK_RE = re.compile(r"!\s*$")
_ENDS_WITH_PUNCT_RE = re.compile(r"[.!?]\s*$")

# ALL CAPS (at least 2 letters A-Z to be considered)
_ALL_CAPS_RE = re.compile(r"^(?=(?:.*[A-Z]){2,})[^a-z]*$")

# starts with lowercase letter (allows whitespace and some opening punctuation first)
_STARTS_WITH_LOWER_RE = re.compile(r"^\s*['\"(\[]*\s*[a-z]")

# separator: no alphanumeric content!! (also matches empty strings!)
_NO_ALNUM_RE = re.compile(r"^[^A-Za-z0-9]*$")

# empty / whitespace-only
_EMPTY_RE = re.compile(r"^\s*$")

# URL heuristic
_URL_RE = re.compile(r"(https?://|www\.)\S+", re.IGNORECASE)

# EXTRAS:
# has number (separated from space)
_HAS_NUMBER_RE = re.compile(r"\b\d+\b")
# has emoticon (not unicode ones, old school ones)
_HAS_EMOJI_RE = re.compile(r"[:;=8xX][\-o\*']?[\)\]\(\[dDpP/\\|]+") # EYES + optional NOSE + MOUTH
# has parentheses
_HAS_PAREN_RE = re.compile(r"[()]")

# VERB START HEURISTICS
# Imperative
_IMPERATIVE_VERBS = [
    "do", "return", "check", "handle", "raise", "prevent", "extract", "ensure", "avoid",
    "use", "set", "get", "compute", "parse", "format", "convert", "create", "build",
    "add", "remove", "update", "write", "read", "load", "save", "fix", "note", "keep",
    "disable", "enable", "skip", "ignore", "allow", "deny", "validate", "guard",
]
_IMPERATIVE_RE = re.compile(r"^\s*['\"(\[]*\s*(?:" + "|".join(map(re.escape, _IMPERATIVE_VERBS)) + r")\b", re.IGNORECASE)

# Descriptive
_DESCRIPTIVE_VERBS = [
    "does", "returns", "checks", "handles", "raises", "prevents", "extracts", "ensures",
    "avoids", "uses", "sets", "gets", "computes", "parses", "formats", "converts",
    "creates", "builds", "adds", "removes", "updates", "writes", "reads", "loads",
    "saves", "fixes", "keeps", "disables", "enables", "skips", "ignores", "allows",
    "validates", "guards",
    # extra opener words (not verbs)
    "this", "these", "it", "they",
]
_DESCRIPTIVE_RE = re.compile(r"^\s*['\"(\[]*\s*(?:" + "|".join(map(re.escape, _DESCRIPTIVE_VERBS)) + r")\b", re.IGNORECASE)


def add_linguistic_features(
    df_blocks: pd.DataFrame,
    text_col: str = "block_text_stripped",
) -> pd.DataFrame:
    """
    Adds lightweight linguistic / stylistic features.

    PREFIX FOR VARIABLES ADDED: lf = linguistic features

    Columns added (bool unless noted):
      - lf_has_punctuation_end
      - lf_ends_with_period
      - lf_is_question
      - lf_is_exclamation

      - lf_is_all_caps
      - lf_starts_with_lowercase
      - lf_starts_with_imperative_verb
      - lf_starts_with_descriptive_verb

      - lf_no_alnum_content        (no alphanumeric characters)
      - lf_is_empty                (empty or whitespace-only)
      - lf_is_separator            (no content but not empty)
      - lf_has_url

      Extra (helpful, optional):
      - lf_has_number
      - lf_has_emoticon
      - lf_has_parentheses

      Summary:
      - lf_number_of_features_true (int)   [counts only the feature booleans above, not including the summary itself]
      - lf_kinds (list[str])              [names of features that are true]
    """
