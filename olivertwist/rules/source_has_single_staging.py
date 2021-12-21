# -*- coding: utf-8 -*-
"""Sources should have a single staging model.

"""

from typing import List, Tuple

from olivertwist.manifest import Manifest, Node
from olivertwist.ruleengine.rule import rule
from olivertwist.rules.utils import partition


@rule(
    id="single-staging-model-per-source",
    name="Sources can only reference a single staging script",
)
def sources_have_single_staging_model(
    manifest: Manifest,
) -> Tuple[List[Node], List[Node]]:
    def source_has_more_than_one_staging_model(node: Node):
        stages = [
            p
            for p in manifest.graph.predecessors(node.id)
            if manifest.get_node(p).is_staging
        ]
        return node.is_source and len(list(stages)) > 1

    passes, failures = partition(
        source_has_more_than_one_staging_model, manifest.nodes()
    )
    return list(passes), list(failures)
