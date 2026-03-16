import pandas as pd
from pathlib import Path

file_path = Path("data/processed/comment_blocks_enriched/core/comment_blocks_enriched.parquet")

df = pd.read_parquet(file_path)

# Overall percentage
overall_percentage = df["sb_is_shebang"].mean() * 100
print(f"Overall: {overall_percentage:.2f}% of rows have sb_is_shebang = True")

# Group-specific percentages
group_percentages = (
    df.groupby("group")["sb_is_shebang"]
    .mean()
    .mul(100)
)

for group in ["old_2000s", "new_2020s"]:
    if group in group_percentages.index:
        print(f"{group}: {group_percentages[group]:.2f}% of rows have sb_is_shebang = True")
    else:
        print(f"{group}: group not found")