import pytest

from olivertwist.manifest import Manifest
from olivertwist.rules.source_has_single_staging import (
    sources_have_single_staging_model,
)


@pytest.fixture
def manifest() -> Manifest:
    return Manifest(
        {
            "nodes": {
                "staging.b": {
                    "unique_id": "staging.b",
                    "fqn": ["staging", "b"],
                    "resource_type": "model",
                },
                "staging.z": {
                    "unique_id": "staging.z",
                    "fqn": ["staging", "z"],
                    "resource_type": "model",
                },
                "staging.y": {
                    "unique_id": "staging.y",
                    "fqn": ["staging", "y"],
                    "resource_type": "model",
                },
            },
            "child_map": {
                "a": ["staging.b"],
                "staging.b": [],
                "x": ["staging.z","staging.y"],
                "staging.y": [],
                "staging.z": [],
            },
            "disabled": [],
            "sources": {
                "a": {
                    "unique_id": "a",
                    "fqn": ["a"],
                    "resource_type": "source",
                },
                "x": {
                    "unique_id": "x",
                    "fqn": ["x"],
                    "resource_type": "source",
                },
                "y": {
                    "unique_id": "y",
                    "fqn": ["y"],
                    "resource_type": "source",
                },
            },
        }
    )


def test_sources_have_single_staging_model_returns_correct_split(manifest):
    passes, failures = sources_have_single_staging_model(manifest)

    pass_ids = [p.id for p in passes]
    failure_ids = [f.id for f in failures]

    assert pass_ids == ["a", "staging.b", "x", "y"]
    assert failure_ids == ["x"]
