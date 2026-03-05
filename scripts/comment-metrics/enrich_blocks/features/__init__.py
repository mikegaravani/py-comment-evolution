from __future__ import annotations

from typing import Callable, List
import pandas as pd

from .annotation_markers import add_annotation_marker_features


FeatureFn = Callable[[pd.DataFrame], pd.DataFrame]


def get_feature_pipeline() -> List[FeatureFn]:
    """
    Ordered list of feature functions to apply.
    """
    return [
        add_annotation_marker_features,
    ]