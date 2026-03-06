# python tests/features/check-more-cross-feature-blocks.py

import pandas as pd
from pathlib import Path

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")

df = pd.read_parquet(file_path)

cols = ["sb_is_shebang", "lh_has_legal_signal", "am_has_annotation_marker", "td_has_tooling_directive", "lf_has_linguistic_feature"]

missing = [c for c in cols if c not in df.columns]
if missing:
    raise KeyError(f"Missing required columns: {missing}")

flags = df[cols].fillna(False).astype(bool)

n = len(flags)
if n == 0:
    raise ValueError("DataFrame has 0 blocks; cannot compute percentages.")

def pct(mask: pd.Series):
    count = int(mask.sum())
    pct = 100.0 * count / n
    return pct, count

results = {
    # Singles
    "sb": pct(flags["sb_is_shebang"]),
    "lh": pct(flags["lh_has_legal_signal"]),
    "am": pct(flags["am_has_annotation_marker"]),

    # Pairs
    "sb & lh": pct(flags["sb_is_shebang"] & flags["lh_has_legal_signal"]),
    "sb & am": pct(flags["sb_is_shebang"] & flags["am_has_annotation_marker"]),
    "lh & am": pct(flags["lh_has_legal_signal"] & flags["am_has_annotation_marker"]),

    # Triple
    "sb & lh & am": pct(
        flags["sb_is_shebang"]
        & flags["lh_has_legal_signal"]
        & flags["am_has_annotation_marker"]
    ),

    # OR Pairs
    "sb | lh": pct(flags["sb_is_shebang"] | flags["lh_has_legal_signal"]),
    "sb | am": pct(flags["sb_is_shebang"] | flags["am_has_annotation_marker"]),
    "lh | am": pct(flags["lh_has_legal_signal"] | flags["am_has_annotation_marker"]),

    # OR Triple
    "sb | lh | am": pct(
        flags["sb_is_shebang"]
        | flags["lh_has_legal_signal"]
        | flags["am_has_annotation_marker"]
    ),
}

# Pretty print
order = [
    "sb",
    "lh",
    "am",
    "sb & lh",
    "sb & am",
    "lh & am",
    "sb & lh & am",
    "sb | lh",
    "sb | am",
    "lh | am",
    "sb | lh | am",
]

print(f"Total blocks: {n}")

for k in order:
    pct, count = results[k]
    print(f"{k:18s}: {pct:6.2f}%  ({count:,} blocks)")

print("\nblock_ids where lh & am is True:")

block_ids = df.loc[flags["lh_has_legal_signal"] & flags["am_has_annotation_marker"], "block_id"]

for bid in block_ids:
    print(f"- {bid}")