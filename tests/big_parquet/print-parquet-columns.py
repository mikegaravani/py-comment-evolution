# python tests/big_parquet/print-parquet-columns.py

import pandas as pd
from pathlib import Path

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")

df = pd.read_parquet(file_path)

for col in df.columns:
    print(col)

print(f"\nTotal columns: {len(df.columns)}")