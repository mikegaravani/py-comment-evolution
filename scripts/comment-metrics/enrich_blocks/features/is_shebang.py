from __future__ import annotations

import re
import pandas as pd

_SHEBANG_RE = re.compile(r"^#!")

def add_shebang_features(
    df_blocks: pd.DataFrame,
    text_col: str = "block_text_raw",
) -> pd.DataFrame:
    """
    Detects shebang lines.
    PREFIX FOR VARIABLES ADDED: sb = shebang

    Columns added:
      - sb_is_shebang (bool)
    """

    if text_col not in df_blocks.columns:
        raise ValueError(f"Missing expected text column: {text_col}")

    s = df_blocks[text_col].fillna("").astype(str)

    is_shebang = s.str.match(_SHEBANG_RE) & (df_blocks["start_lineno"] == 1)

    out = df_blocks.copy()

    out["sb_is_shebang"] = is_shebang

    return out