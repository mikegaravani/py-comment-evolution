# python tests/features/check-am.py

import pandas as pd
from pathlib import Path
import numpy as np

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")

df = pd.read_parquet(file_path)

df["am_kinds"] = df["am_kinds"].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

# boolean am_* flags
am_bool_cols = [
    c for c in df.columns
    if c.startswith("am_") and df[c].dropna().isin([True, False]).all()
]

if not am_bool_cols:
    print("No boolean am_* columns found.")
else:
    print("Percent of blocks where each am_* flag is True:")
    for c in sorted(am_bool_cols):
        pct_true = 100.0 * df[c].fillna(False).mean()
        print(f"  {c}: {pct_true:.2f}%")

# non-boolean am_* characteristics summarized as % non-empty / >0
am_other_cols = [c for c in df.columns if c.startswith("am_") and c not in am_bool_cols]

def pct_nonempty(series: pd.Series) -> float:
    s = series
    if pd.api.types.is_numeric_dtype(s):
        return 100.0 * (s.fillna(0) > 0).mean()
    def is_nonempty(x) -> bool:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return False
        if isinstance(x, (list, tuple, set, dict, str, bytes)):
            return len(x) > 0
        return True
    return 100.0 * s.map(is_nonempty).mean()

if am_other_cols:
    print("\nPercent of blocks where each non-boolean am_* field is non-empty / >0:")
    for c in sorted(am_other_cols):
        pct = pct_nonempty(df[c])
        print(f"  {c}: {pct:.2f}%")