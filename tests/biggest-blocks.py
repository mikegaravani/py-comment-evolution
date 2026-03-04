# python tests/biggest-blocks.py

import pandas as pd
from pathlib import Path

file_path = Path("data/processed/comment_blocks/core/comment_blocks.parquet")

df = pd.read_parquet(file_path)

filtered = df[df["block_kind"] == "full_line_block"]

top10 = filtered.sort_values("n_lines", ascending=False).head(10)

for _, row in top10.iterrows():
    print(f"n_lines={row['n_lines']} | repo={row['repo']} | path_rel={row['path_rel']}")