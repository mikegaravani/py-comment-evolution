# python tests/results/check-td-2000s-with-pragma.py

import pandas as pd
from pathlib import Path

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")

# Load parquet
df = pd.read_parquet(file_path)

# Filter rows
filtered = df[(df["group"] == "old_2000s") & (df["td_has_pragma"] == True)] # noqa: E712

# Print block ids
for block_id in filtered["block_id"]:
    print(block_id)