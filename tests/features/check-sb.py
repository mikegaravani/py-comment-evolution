# python tests/features/check-sb.py

import pandas as pd
from pathlib import Path

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")

df = pd.read_parquet(file_path)

percentage = df["sb_is_shebang"].mean() * 100

print(f"{percentage:.2f}% of rows have sb_is_shebang = True")

