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

feature_count = (
    flags["sb_is_shebang"].astype(int) +
    flags["lh_has_legal_signal"].astype(int) +
    flags["am_has_annotation_marker"].astype(int) +
    flags["td_has_tooling_directive"].astype(int) +
    flags["lf_has_linguistic_feature"].astype(int)
)

at_least_3_mask = feature_count >= 3

results = {
    # Singles
    "sb": pct(flags["sb_is_shebang"]),
    "lh": pct(flags["lh_has_legal_signal"]),
    "am": pct(flags["am_has_annotation_marker"]),
    "td": pct(flags["td_has_tooling_directive"]),
    "lf": pct(flags["lf_has_linguistic_feature"]),

    # AT LEAST ONE
    "sb | lh | am | td | lf": pct(
        flags["sb_is_shebang"] |
        flags["lh_has_legal_signal"] |
        flags["am_has_annotation_marker"] |
        flags["td_has_tooling_directive"] |
        flags["lf_has_linguistic_feature"]
    ),

    # AT LEAST TWO
    "at least 2": pct(
        (flags["sb_is_shebang"].astype(int) +
         flags["lh_has_legal_signal"].astype(int) +
         flags["am_has_annotation_marker"].astype(int) +
         flags["td_has_tooling_directive"].astype(int) +
         flags["lf_has_linguistic_feature"].astype(int)) >= 2
    ),

    # AT LEAST THREE
    "at least 3": pct(at_least_3_mask),

    # AT LEAST FOUR
    "at least 4": pct(
        (flags["sb_is_shebang"].astype(int) +
         flags["lh_has_legal_signal"].astype(int) +
         flags["am_has_annotation_marker"].astype(int) +
         flags["td_has_tooling_directive"].astype(int) +
         flags["lf_has_linguistic_feature"].astype(int)) >= 4
    ),

    # ALL 5
    "sb & lh & am & td & lf": pct(
        flags["sb_is_shebang"] &
        flags["lh_has_legal_signal"] &
        flags["am_has_annotation_marker"] &
        flags["td_has_tooling_directive"] &
        flags["lf_has_linguistic_feature"]
    ),
}

# Pretty print
order = [
    "sb",
    "lh",
    "am",
    "td",
    "lf",
    "sb | lh | am | td | lf",
    "at least 2",
    "at least 3",
    "at least 4",
    "sb & lh & am & td & lf",
]

print(f"Total blocks: {n}")

for k in order:
    pct, count = results[k]
    print(f"{k:30s}: {pct:6.2f}%  ({count:,} blocks)")

print("\n--- Blocks with at least 3 features ---\n")

rows = df.loc[at_least_3_mask, "block_text_stripped"]

for i, (idx, text) in enumerate(rows.items(), 1):
    row_flags = flags.loc[idx]

    active = [col for col, val in row_flags.items() if val]

    print(f"\n[{i}]", "features:", ", ".join(active), "\n")
    print(text)