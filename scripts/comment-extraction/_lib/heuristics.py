'''
IMPORTANT NOTE: THIS IS ALL EXPANDABLE. THIS IS ONLY A STARTING POINT
'''
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


URL_RE = re.compile(r"\bhttps?://\S+|\bwww\.\S+", re.IGNORECASE)

TODO_RE = re.compile(r"\bTODO\b", re.IGNORECASE)
FIXME_RE = re.compile(r"\bFIXME\b", re.IGNORECASE)
HACK_RE = re.compile(r"\bHACK\b", re.IGNORECASE)

PRAGMATIC_PATTERNS = [
    ("noqa", re.compile(r"\bnoqa\b", re.IGNORECASE)),
    ("pylint", re.compile(r"\bpylint:\s*disable\b|\bpylint:\s*enable\b", re.IGNORECASE)),
    ("fmt", re.compile(r"\bfmt:\s*off\b|\bfmt:\s*on\b", re.IGNORECASE)),
    ("black", re.compile(r"\bnoqa:\s*black\b|\bblack:\s*off\b|\bblack:\s*on\b", re.IGNORECASE)),
    ("isort", re.compile(r"\bisort:\s*skip\b|\bisort:\s*off\b|\bisort:\s*on\b", re.IGNORECASE)),
    ("mypy", re.compile(r"\bmypy:\s*(ignore-errors|disable-error-code|enable-error-code)\b", re.IGNORECASE)),
    ("pyright", re.compile(r"\bpyright:\s*(ignore|report)\b|\b#\s*pyright:\s*", re.IGNORECASE)),
]

TYPE_COMMENT_RE = re.compile(
    r"^\s*(type:\s*ignore(\[[^\]]+\])?|type:\s*[^#]+)\s*$",
    re.IGNORECASE
)

# ROUGH START for heuristics to detect if a comment looks like it might be commented-out code.
CODEY_TOKENS_RE = re.compile(
    r"\b(def|class|import|from|return|yield|if|elif|else|for|while|try|except|with|assert|raise)\b"
)
ASSIGNMENT_RE = re.compile(r"(^|\s)[A-Za-z_]\w*\s*=\s*[^=]")
PUNCT_HEAVY_RE = re.compile(r"[{}();]")

PRAGMATIC_HINTS_RE = re.compile(
    r"(noqa|pylint:|fmt:|black:|isort:|mypy:|pyright:)", re.IGNORECASE
)


@dataclass(frozen=True)
class CommentFeatures:
    has_url: bool
    has_todo: bool
    has_fixme: bool
    has_hack: bool

    starts_with_capital: bool

    looks_like_commented_code: bool

    is_pragmatic: bool
    pragmatic_kinds: str

    is_type_comment: bool


def starts_with_capital(text: str) -> bool:
    s = text.lstrip()
    if not s:
        return False
    # first alphabetic char capital? TODO remove this part?
    for ch in s:
        if ch.isalpha():
            return ch.isupper()
        if ch.isdigit():
            return False
    return False


# TODO important!! This will have to be tuned and improved
def looks_like_commented_code(text: str) -> bool:
    """
    Heuristic: comment content resembles code.
    Tune over time; keep it conservative.
    """
    s = text.strip()
    if not s:
        return False

    # If it contains clear tooling directive or type directive, don't treat as code
    if PRAGMATIC_HINTS_RE.search(s) or TYPE_COMMENT_RE.match(s):
        return False

    score = 0

    if CODEY_TOKENS_RE.search(s):
        score += 2
    if ASSIGNMENT_RE.search(s):
        score += 2
    if PUNCT_HEAVY_RE.search(s):
        score += 1
    if "->" in s or "::" in s:
        score += 1
    if s.endswith(":"):
        score += 1
    if s.count("(") + s.count(")") >= 2:
        score += 1

    # Symbol ratio
    symbols = sum(1 for c in s if not c.isalnum() and not c.isspace())
    if len(s) > 0 and symbols / len(s) > 0.25:
        score += 1

    return score >= 3


def pragmatic_kinds(text: str) -> list[str]:
    kinds = []
    for name, rx in PRAGMATIC_PATTERNS:
        if rx.search(text):
            kinds.append(name)
    return kinds


def extract_features(comment_text: str) -> CommentFeatures:
    # comment_text should already be stripped of the leading '#'!!!
    s = comment_text.strip()

    has_url = bool(URL_RE.search(s))
    has_todo = bool(TODO_RE.search(s))
    has_fixme = bool(FIXME_RE.search(s))
    has_hack = bool(HACK_RE.search(s))

    capital = starts_with_capital(s)

    prag_kinds = pragmatic_kinds(s)
    is_prag = len(prag_kinds) > 0

    is_type = bool(TYPE_COMMENT_RE.match(s))

    codey = looks_like_commented_code(s)

    return CommentFeatures(
        has_url=has_url,
        has_todo=has_todo,
        has_fixme=has_fixme,
        has_hack=has_hack,
        starts_with_capital=capital,
        looks_like_commented_code=codey,
        is_pragmatic=is_prag,
        pragmatic_kinds="|".join(prag_kinds),
        is_type_comment=is_type,
    )