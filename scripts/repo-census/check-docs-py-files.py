import pandas as pd
from pathlib import Path

dir_path = Path("data/processed/inventory/")

for file_path in dir_path.glob("*.csv"):
    try:
        df = pd.read_csv(file_path)

        if "is_docs" in df.columns and "is_py" in df.columns:
            count = df[(df["is_docs"] == True) & (df["is_py"] == True)].shape[0] # noqa: E712
            print(f"{file_path.name}: {count}")
        else:
            print(f"{file_path.name}: 'is_docs' or 'is_py' column not found")

    except Exception as e:
        print(f"Error processing {file_path.name}: {e}")