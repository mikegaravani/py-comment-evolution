from __future__ import annotations

from typing import Callable, List
import pandas as pd

from .annotation_markers import add_annotation_marker_features
from .legal_headers import add_legal_header_features
from .is_shebang import add_shebang_features
from .tooling_directives import add_tooling_directive_features
from .linguistic_features import add_linguistic_features


FeatureFn = Callable[[pd.DataFrame], pd.DataFrame]


def get_feature_pipeline() -> List[FeatureFn]:
    """
    Ordered list of feature functions to apply.
    """
    return [
        add_shebang_features,
        add_legal_header_features,
        add_annotation_marker_features,
        add_tooling_directive_features,
        add_linguistic_features,
    ]