# python tests/check-text-vs-text-stripped.py

import pandas as pd
from pathlib import Path

file_path = Path("data/processed/tokenized_data/core/comments_token.parquet")

df = pd.read_parquet(file_path)

mask = df["raw_token"].fillna("") != df["line_text"].fillna("")
count = mask.sum()

print("Instances where raw_token != line_text:", count)

if count > 0:
    first_row = df.loc[mask].iloc[0]

    print("\nFirst mismatch:")
    print("Path:", first_row["path_rel"])
    print("raw token:", first_row["raw_token"])
    print("line text:", first_row["line_text"])