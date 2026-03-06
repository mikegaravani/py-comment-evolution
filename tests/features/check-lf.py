# python tests/features/check-lf.py

import pandas as pd
from pathlib import Path
import numpy as np

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")

df = pd.read_parquet(file_path)

df["lf_kinds"] = df["lf_kinds"].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
df["lf_emoticons_found"] = df["lf_emoticons_found"].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

# boolean lf_* flags
lf_bool_cols = [
    c for c in df.columns
    if c.startswith("lf_") and df[c].dropna().isin([True, False]).all()
]

if not lf_bool_cols:
    print("No boolean lf_* columns found.")
else:
    print("Percent of blocks where each lf_* flag is True:")
    for c in sorted(lf_bool_cols):
        pct_true = 100.0 * df[c].fillna(False).mean()
        print(f"  {c}: {pct_true:.2f}%")

# non-boolean lf_* characteristics summarized as % non-empty / >0
lf_other_cols = [c for c in df.columns if c.startswith("lf_") and c not in lf_bool_cols]

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

if lf_other_cols:
    print("\nPercent of blocks where each non-boolean lf_* field is non-empty / >0:")
    for c in sorted(lf_other_cols):
        pct = pct_nonempty(df[c])
        print(f"  {c}: {pct:.2f}%")


# print emoticons captured
if "lf_emoticons_found" not in df.columns:
    print("\nColumn lf_emoticons_found not found.")
else:
    emoticons = (
        df["lf_emoticons_found"]
        .explode()
        .dropna()
    )

    emoticons = emoticons[emoticons.astype(str).str.len() > 0]

    if emoticons.empty:
        print("\nNo emoticons captured.")
    else:
        print("\nCaptured emoticons (counts):")
        counts = emoticons.value_counts()
        for emoticon, count in counts.items():
            print(f"  {emoticon} {count}")