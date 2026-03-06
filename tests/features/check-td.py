# python tests/features/check-td.py

import pandas as pd
from pathlib import Path
import numpy as np

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")

df = pd.read_parquet(file_path)

df["td_kinds"] = df["td_kinds"].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

# boolean td_* flags
td_bool_cols = [
    c for c in df.columns
    if c.startswith("td_") and df[c].dropna().isin([True, False]).all()
]

if not td_bool_cols:
    print("No boolean td_* columns found.")
else:
    print("Percent of blocks where each td_* flag is True:")
    for c in sorted(td_bool_cols):
        pct_true = 100.0 * df[c].fillna(False).mean()
        print(f"  {c}: {pct_true:.2f}%")

# non-boolean td_* characteristics summarized as % non-empty / >0
td_other_cols = [c for c in df.columns if c.startswith("td_") and c not in td_bool_cols]

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

if td_other_cols:
    print("\nPercent of blocks where each non-boolean td_* field is non-empty / >0:")
    for c in sorted(td_other_cols):
        pct = pct_nonempty(df[c])
        print(f"  {c}: {pct:.2f}%")