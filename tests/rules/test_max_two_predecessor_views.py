import pytest

from olivertwist.manifest import Manifest
from olivertwist.rules.max_two_predecessor_views     import models_have_more_than_two_predecessor_views


@pytest.fixture
def manifest() -> Manifest:
    return Manifest(
        {
            "nodes": {
                "chain_1_table_a": {
                    "unique_id": "chain_1_table_a",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
                "chain_1_table_b": {
                    "unique_id": "chain_1_table_b",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
                "chain_2_table_a": {
                    "unique_id": "chain_2_table_a",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
                "chain_2_view_b": {
                    "unique_id": "chain_2_view_b",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "view",
                        },
                },
                "chain_2_table_c": {
                    "unique_id": "chain_2_table_c",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
                "chain_3_table_a": {
                    "unique_id": "chain_3_table_a",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
                "chain_3_view_b": {
                    "unique_id": "chain_3_view_b",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "view",
                        },
                },
                "chain_3_view_c": {
                    "unique_id": "chain_3_view_c",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "view",
                        },
                },
                "chain_3_view_d": {
                    "unique_id": "chain_3_view_d",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "view",
                        },
                },
                "chain_3_incremental_e": {
                    "unique_id": "chain_3_incremental_e",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "incremental",
                        },
                },
                "chain_3_table_f": {
                    "unique_id": "chain_3_table_f",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
                "chain_4_table_a": {
                    "unique_id": "chain_4_table_a",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
                "chain_4_table_b": {
                    "unique_id": "chain_4_table_b",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
                "chain_4_view_c": {
                    "unique_id": "chain_4_view_c",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "view",
                        },
                },
                "chain_4_view_d": {
                    "unique_id": "chain_4_view_d",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "view",
                        },
                },
                "chain_4_view_e": {
                    "unique_id": "chain_4_view_e",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "view",
                        },
                },
                "chain_4_table_f": {
                    "unique_id": "chain_4_table_f",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                        },
                },
            },
            "child_map": {
                "chain_1_table_a": ["chain_1_table_b"],
                "chain_1_table_b": [],
                "chain_2_table_a": ["chain_2_view_b"],
                "chain_2_view_b": ["chain_2_table_c"],
                "chain_2_table_c": [],
                "source_1": ["chain_3_table_a"],
                "chain_3_table_a": ["chain_3_view_b"],
                "chain_3_view_b": ["chain_3_view_c"],
                "chain_3_view_c": ["chain_3_view_d"],
                "chain_3_view_d": ["chain_3_incremental_e"],
                "chain_3_incremental_e": ["chain_3_table_f"],
                "chain_3_table_f": [],
                "chain_4_table_a": ["chain_4_table_b"],
                "chain_4_table_b": ["chain_4_view_c"],
                "chain_4_view_c": ["chain_4_view_d"],
                "chain_4_view_d": ["chain_4_view_e"],
                "chain_4_view_e": ["chain_4_table_f"],
                "chain_4_table_f": [],
            },
            "disabled": [],
            "sources": {
                "source_1": {
                    "unique_id": "source_1",
                    "resource_type": "source",
                },
            },

        }
    )


def test_models_have_more_than_two_predecessor_views(manifest):
    passes, failures = models_have_more_than_two_predecessor_views(manifest)

    pass_ids = [p.id for p in passes]
    failure_ids = [f.id for f in failures]

    assert pass_ids == [
        "chain_1_table_a",
        "chain_1_table_b",
        "chain_2_table_a",
        "chain_2_view_b",
        "chain_2_table_c",
        "source_1",
        "chain_3_table_a",
        "chain_3_view_b",
        "chain_3_view_c",
        "chain_3_view_d",
        "chain_3_table_f",
        "chain_4_table_a",
        "chain_4_table_b",
        "chain_4_view_c",
        "chain_4_view_d",
        "chain_4_view_e",
    ]
    assert failure_ids == [
        "chain_3_incremental_e",
        "chain_4_table_f",
    ]
