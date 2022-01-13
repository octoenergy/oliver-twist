import pytest

from olivertwist.manifest import Manifest
from olivertwist.rules.no_owner_on_physical_models import no_owner_on_physical_models


@pytest.fixture
def manifest() -> Manifest:
    return Manifest(
        {
            "nodes": {
                "physical_node_1": {
                    "unique_id": "physical_node_1",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "view",
                            "meta":
                                {
                                    "owner": "Oliver Twist",
                                    "team_owner": "The Juvenile Pickpockets"
                                },
                        },
                },
                "physical_node_2": {
                    "unique_id": "physical_node_2",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "table",
                            "meta":
                                {
                                    "owner": "Artful Dodger",
                                    "team_owner": "The Juvenile Pickpockets"
                                },
                        },
                },
                "physical_node_3": {
                    "unique_id": "physical_node_3",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "incremental",
                            "meta":
                                {
                                    "owner": "Charley Bates",
                                    "team_owner": "The Juvenile Pickpockets"
                                },
                        },
                },
                "ephemeral_node_1": {
                    "unique_id": "ephemeral_node_1",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "ephemeral",
                            "meta":
                                {
                                    "owner": "Oliver Twist",
                                    "team_owner": "The Juvenile Pickpockets"
                                },
                        },
                },
                "ephemeral_node_2": {
                    "unique_id": "ephemeral_node_2",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "ephemeral",
                            "meta":
                                {},
                        },
                },
                "no_owner_physical_node_1": {
                    "unique_id": "no_owner_physical_node_1",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "ephemeral",
                            "meta":
                                {
                                    "owner": "",
                                    "team_owner": ""
                                },
                        },
                },
                "no_owner_physical_node_2": {
                    "unique_id": "no_owner_physical_node_2",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "incremental",
                            "meta":
                                {
                                    "owner": "",
                                    "team_owner": ""
                                },
                        },
                },
                "no_owner_physical_node_3": {
                    "unique_id": "no_owner_physical_node_3",
                    "resource_type": "model",
                    "config":
                        {
                            "materialized": "incremental",
                            "meta":
                                {
                                    "owner": "   ",
                                    "team_owner": "   "
                                },
                        },
                },
            },
            "child_map": {
                "physical_node_1": [],
                "physical_node_2": [],
                "physical_node_3": [],
                "ephemeral_node_1": [],
                "ephemeral_node_2": [],
                "no_owner_physical_node_1": [],
                "no_owner_physical_node_2": [],
                "no_owner_physical_node_3": [],
                "source_1": [],
                "no_owner_source_1": [],
                "no_owner_source_2": [],
                "no_owner_source_3": [],
            },
            "disabled": [],
            "sources": {
                "source_1": {
                    "unique_id": "source_1",
                    "resource_type": "source",
                    "config":
                        {"meta": {"owner": "Oliver Twist"}}
                },
                "no_owner_source_1": {
                    "unique_id": "no_owner_source_1",
                    "resource_type": "source",
                    "config":
                        {"meta": {"owner": ""}}
                },
                "no_owner_source_2": {
                    "unique_id": "no_owner_source_2",
                    "resource_type": "source",
                    "config":
                        {"meta": {"owner": "  "}}
                },
                "no_owner_source_3": {
                    "unique_id": "no_owner_source_3",
                    "resource_type": "source",
                    "config":
                        {"meta": {"owner": "  "}}
                },

            },

        }
    )


def test_no_owner_on_physical_models(manifest):
    passes, failures = no_owner_on_physical_models(manifest)

    pass_ids = [p.id for p in passes]
    failure_ids = [f.id for f in failures]

    assert pass_ids == [
        "physical_node_1",
        "physical_node_2",
        "physical_node_3",
        "ephemeral_node_1",
        "ephemeral_node_2",
        "source_1"
    ]
    assert failure_ids == [
        "no_owner_physical_node_1",
        "no_owner_physical_node_2",
        "no_owner_physical_node_3",
        "no_owner_source_1",
        "no_owner_source_2",
        "no_owner_source_3",
    ]
