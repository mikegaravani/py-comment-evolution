# python tests/big_parquet/output-file-with-parquet-columns.py

import pyarrow.parquet as pq
from pathlib import Path

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")
output_path = Path("data/metadata/comment_block_parquet_columns.csv")

schema = pq.read_schema(file_path)

with open(output_path, "w") as f:
    f.write("column_name\n")
    for name in schema.names:
        f.write(name + "\n")