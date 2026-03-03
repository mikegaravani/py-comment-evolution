'''
Find files that gave an error when trying to read docstrings (ast or parso).

USAGE:
    python tests/read-error-docstrings.py 
'''      

import pandas as pd
from pathlib import Path

file_path = Path("data/processed/docstring_data/core/file_status_docstrings.parquet")

df = pd.read_parquet(file_path)

count = df["read_error"].notna().sum()

print(count)