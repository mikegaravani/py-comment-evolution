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
_URL_RE = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)

# EXTRAS:
# has number (separated from space)
_HAS_NUMBER_RE = re.compile(r"\b\d+\b")
# has emoticon (not unicode ones, old school ones)
_HAS_EMOJI_RE = re.compile(
    r"(?:^|\s)[:;=8xX][\-o\*']?[\)\]\(\[dDpP/\\|]+(?=\s|$|[.,;:!?])" # whitespace before and allowed punctuation after
)
# has parentheses (round)
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
    Adds linguistic / stylistic features.
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

        - lf_has_no_alphanumeric        (no alphanumeric characters)
        - lf_is_empty                (empty or whitespace-only)
        - lf_is_separator            (no alnum but not empty)
        - lf_has_url

        - lf_has_number
        - lf_has_emoticon
        - lf_has_parentheses

      Summary:
        - lf_has_linguistic_feature (bool)  [true if any of the above features are true]
        - lf_number_of_features_true (int)
        - lf_kinds (list[str])
    """
    if text_col not in df_blocks.columns:
        raise ValueError(f"Missing expected text column: {text_col}")

    # normalize minimally; keep your canonical stripped text
    s_raw = df_blocks[text_col].fillna("").astype(str)
    s = s_raw.str.strip()

    features = {
        # punctuation
        "has_punctuation_end": s.str.contains(_ENDS_WITH_PUNCT_RE),
        "ends_with_period": s.str.contains(_ENDS_WITH_PERIOD_RE),
        "is_question": s.str.contains(_ENDS_WITH_QMARK_RE),
        "is_exclamation": s.str.contains(_ENDS_WITH_EMARK_RE),

        "is_all_caps": s.str.contains(_ALL_CAPS_RE),
        "starts_with_lowercase": s.str.contains(_STARTS_WITH_LOWER_RE),

        "starts_with_imperative_verb": s.str.contains(_IMPERATIVE_RE),
        "starts_with_descriptive_verb": s.str.contains(_DESCRIPTIVE_RE),

        "has_no_alphanumeric": s.str.match(_NO_ALNUM_RE),
        "is_empty": s.str.match(_EMPTY_RE),
        "is_separator": s.str.match(_NO_ALNUM_RE) & ~s.str.match(_EMPTY_RE),
        "has_url": s.str.contains(_URL_RE),

        "has_number": s.str.contains(_HAS_NUMBER_RE),
        "has_emoticon": s.str.contains(_HAS_EMOJI_RE),
        "has_parentheses": s.str.contains(_HAS_PAREN_RE),
    }

    df_matches = pd.DataFrame(features)

    has_any = df_matches.any(axis=1)

    redundant_punct = (
        df_matches["has_punctuation_end"]
        & (
            df_matches["ends_with_period"]
            | df_matches["is_question"]
            | df_matches["is_exclamation"]
        )
    )
    number_of_features_true = df_matches.sum(axis=1).astype(int) - redundant_punct.astype(int) # remove redundant punctuation count

    kinds = df_matches.apply(
        lambda row: [name for name, present in row.items() if present], axis=1
    )

    out = df_blocks.copy()

    for name in features:
        out[f"lf_{name}"] = df_matches[name]

    out["lf_has_linguistic_feature"] = has_any
    out["lf_number_of_features_true"] = number_of_features_true
    out["lf_kinds"] = kinds

    return out