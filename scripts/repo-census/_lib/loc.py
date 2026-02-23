from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class LocResult:
    is_binary: bool
    encoding_error: bool
    loc_total: int
    loc_blank: int


def _looks_binary(path: Path, sample_size: int = 8192) -> bool:
    try:
        with path.open("rb") as f:
            chunk = f.read(sample_size)
        return b"\x00" in chunk
    except OSError:
        return True


def _read_text(path: Path) -> tuple[Optional[str], bool]:
    """
    Read file as text.
    Returns (text, encoding_error).
    """
    try:
        text = path.read_text(encoding="utf-8")
        return text, False
    except UnicodeDecodeError:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            return text, True
        except OSError:
            return None, True
    except OSError:
        return None, True


def count_loc_text(text: str) -> tuple[int, int]:
    '''
    Returns total LOC and blank LOC
    '''
    lines = text.splitlines()
    loc_total = len(lines)
    loc_blank = sum(1 for ln in lines if ln.strip() == "")
    return loc_total, loc_blank


def compute_loc(path: Path) -> LocResult:
    """
    Compute physical LOC + blank LOC for any text file.
    Binary files return loc_total=0/loc_blank=0 with is_binary=True.
    """
    if _looks_binary(path):
        return LocResult(is_binary=True, encoding_error=False, loc_total=0, loc_blank=0)

    text, enc_err = _read_text(path)
    if text is None:
        return LocResult(is_binary=False, encoding_error=True, loc_total=0, loc_blank=0)

    loc_total, loc_blank = count_loc_text(text)
    return LocResult(is_binary=False, encoding_error=enc_err, loc_total=loc_total, loc_blank=loc_blank)