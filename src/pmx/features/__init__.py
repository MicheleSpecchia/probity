"""Feature engineering helpers."""

from typing import Any

__all__ = ["FEATURE_SET_VERSION", "MicroFeatureStore", "compute_micro_v1_features"]


def __getattr__(name: str) -> Any:
    if name == "FEATURE_SET_VERSION":
        from pmx.features.spec_micro_v1 import FEATURE_SET_VERSION

        return FEATURE_SET_VERSION
    if name == "compute_micro_v1_features":
        from pmx.features.spec_micro_v1 import compute_micro_v1_features

        return compute_micro_v1_features
    if name == "MicroFeatureStore":
        from pmx.features.microstore import MicroFeatureStore

        return MicroFeatureStore
    raise AttributeError(name)
