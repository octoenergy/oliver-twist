# -*- coding: utf-8 -*-
"""
Table/incremental materialized models should have a maximum of two views as predecessors before
being materialized as tables. If this is not enforced Spark can struggle with creating query
plans for long lineage chains of views.

This test is a little more complex than normal as it involves tracing the lineage from a model
through multiple node layers whereas normally we only look back at a single predecessor or
successor. Using networkx lineage we need to look at preceding nodes up to 3 layers back.
If we have a consistent chain of 3 views at this stage we can fail the test.
"""

from typing import List, Tuple

from olivertwist.manifest import Manifest, Node
from olivertwist.ruleengine.rule import rule
from olivertwist.rules.utils import partition
import functools


def get_predecessor_layer_view_nodes(
    layer_depth: int,
    manifest: Manifest,
    layer_nodes: List[str],
) -> Tuple[List[str], int]:
    predecessor_layer_nodes = []
    for node in layer_nodes:
        for node_predecessor_node in manifest.graph.predecessors(node):
            if manifest.get_node(node_predecessor_node).materialization in ['view', 'ephemeral']:
                predecessor_layer_nodes.append(node_predecessor_node)
                layer_depth = max(layer_depth, layer_depth+1)
    return predecessor_layer_nodes, layer_depth


def model_has_more_than_x_predecessor_views(
        view_depth_limit: int,
        manifest: Manifest,
        node: Node,
) -> bool:
    current_layer_nodes = [node.id]
    max_layer_depth = 0
    for i in range(view_depth_limit+1):
        current_layer_nodes, max_layer_depth = get_predecessor_layer_view_nodes(
            max_layer_depth,
            manifest,
            current_layer_nodes,
        )
        if max_layer_depth > view_depth_limit:
            return True
    return False

@rule(
    id="max-two-predecessor-views",
    name="Models can only have two or less predecessor views in a chain",
)
def models_have_more_than_two_predecessor_views(
    manifest: Manifest,
    view_depth_limit: int = 2
) -> Tuple[List[Node], List[Node]]:

    model_has_more_than_given_predecessor_views = functools.partial(
        model_has_more_than_x_predecessor_views,
        view_depth_limit,
        manifest
    )

    passes, failures = partition(
        model_has_more_than_given_predecessor_views, manifest.nodes()
    )
    return list(passes), list(failures)
