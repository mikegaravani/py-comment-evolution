from __future__ import annotations

import pandas as pd


REPO_GROUP_COLS = [
    "repo",
    "group",
    "release",
]


def compute_legal_metrics(
    blocks_df: pd.DataFrame,
    subset: str,
) -> pd.DataFrame:
    """
    Compute repo-level legal comment metrics from enriched comment blocks.

    Outputs:
    - repo level only

    Focus:
    - total block count
    - legal-signal block count
    - legal-signal block ratio
    """

    df = blocks_df.copy()

    # -------------------------
    # DEBUG: print legal blocks
    # -------------------------
    # debug_df = df[
    #     (df["lh_has_legal_signal"] == True) &
    #     (~df["repo"].isin(["scons", "ipython"]))
    # ]

    # for _, row in debug_df.iterrows():
    #     print(f"\n[DEBUG] repo={row['repo']} block_id={row.get('block_id')}")
    #     print(row["block_text_raw"])
    #     print("-" * 80)

    repo_level_df = (
        df.groupby(REPO_GROUP_COLS, as_index=False)
        .agg(
            total_blocks=("block_id", "count"),
            legal_blocks=("lh_has_legal_signal", "sum"),
        )
    )

    repo_level_df["subset"] = subset
    repo_level_df["legal_block_ratio"] = (
        repo_level_df["legal_blocks"] / repo_level_df["total_blocks"]
    )

    repo_level_out = repo_level_df[
        [
            "subset",
            "repo",
            "group",
            "release",
            "total_blocks",
            "legal_blocks",
            "legal_block_ratio",
        ]
    ].copy()

    return repo_level_out