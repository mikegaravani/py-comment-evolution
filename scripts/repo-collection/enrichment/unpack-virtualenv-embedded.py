"""
Unpack embedded/compressed files in the virtualenv source tree.

On the SWH virtualenv history, only releases are available, no version control history before 2011.
In these releases (we picked 1.4), some files are compressed in the source code. This script makes sure that
the compressed files are materialized in the output directory.

Patterns to be scanned:
  ##file deactivate.bat
  DEACTIVATE_BAT = '''
  <base64...>
  '''.decode("base64").decode("zlib")

USAGE:
  python scripts/repo-collection/enrichment/unpack-virtualenv-embedded.py
"""

from __future__ import annotations

import base64
import gzip
import re
import zlib
from pathlib import Path


OUT_ROOT = Path("data/raw/software_heritage")
REPO = "virtualenv"
WINDOW = 10_000


FILE_MARKER_RE = re.compile(r"(?m)^[ \t]*##file[ \t]+(?P<path>.+?)[ \t]*$")

EMBEDDED_BLOCK_RE = re.compile(
    r"""
    ^[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*=[ \t]*
    (?P<q>['"]{3})
    (?P<payload>.*?)
    (?P=q)
    (?P<chain>(?:\s*\.\s*decode\s*\(\s*["'][^"']+["']\s*\)\s*)+)
    """,
    re.DOTALL | re.MULTILINE | re.VERBOSE,
)

B64_CALL_RE = re.compile(
    r"""
    base64\s*\.\s*b64decode\s*\(\s*
    (?P<q>['"]{3})
    (?P<payload>.*?)
    (?P=q)
    \s*\)
    """,
    re.DOTALL | re.VERBOSE,
)


def safe_relpath(rel: str) -> str:
    rel = rel.replace("\\", "/").strip()
    while rel.startswith("/"):
        rel = rel[1:]
    parts = Path(rel).parts
    if any(p == ".." for p in parts):
        raise ValueError(f"Unsafe relative path: {rel}")
    return rel


def decode_payload(b64_text: str, chain: str) -> bytes:
    raw = "".join(b64_text.split()).encode("ascii", errors="ignore")
    data = base64.b64decode(raw)

    chain_l = (chain or "").lower()
    wants_zlib = "zlib" in chain_l
    wants_gzip = "gzip" in chain_l

    if wants_zlib:
        try:
            return zlib.decompress(data)
        except Exception:
            pass

    if wants_gzip:
        try:
            return gzip.decompress(data)
        except Exception:
            pass

    for fn in (
        lambda b: zlib.decompress(b),
        lambda b: zlib.decompress(b, wbits=-zlib.MAX_WBITS),
        lambda b: gzip.decompress(b),
    ):
        try:
            return fn(data)
        except Exception:
            continue

    return data


def scan_one_py(text: str):
    """Yield (rel_out_path, payload, chain) for each ##file marker in this file."""
    for m in FILE_MARKER_RE.finditer(text):
        out_path = safe_relpath(m.group("path"))
        window = text[m.end() : m.end() + WINDOW]

        b = EMBEDDED_BLOCK_RE.search(window)
        if b:
            yield out_path, b.group("payload"), b.group("chain") or ""
            continue

        b2 = B64_CALL_RE.search(window)
        if b2:
            yield out_path, b2.group("payload"), "base64"


def main() -> None:
    repo_root = OUT_ROOT / REPO
    if not repo_root.exists():
        raise SystemExit(f"Missing repo directory: {repo_root}")

    found = 0
    written = 0

    for release_root in sorted([p for p in repo_root.iterdir() if p.is_dir()]):
        print(f"\nScanning: {release_root}")
        for py in release_root.rglob("*.py"):
            if not py.is_file():
                continue

            try:
                text = py.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                print(f"  WARN: cannot read {py}: {e}")
                continue

            for rel_out, payload, chain in scan_one_py(text):
                found += 1
                out_path = release_root / rel_out
                out_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    data = decode_payload(payload, chain)
                except Exception as e:
                    print(f"  WARN: decode failed for {out_path} from {py}: {e}")
                    continue

                if out_path.exists():
                    # don't overwrite existing files
                    continue

                out_path.write_bytes(data)
                written += 1
                print(f"  WROTE {out_path} ({len(data)} bytes) from {py}")

        print(f"Release done: {release_root.name}")

    print(f"\nDone. embedded_markers_found={found} files_written={written}")


if __name__ == "__main__":
    main()
