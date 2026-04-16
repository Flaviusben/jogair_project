"""Build and persist the Aarhus pedestrian street graph."""

from __future__ import annotations

import logging
from pathlib import Path

import osmnx as ox


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_output_path() -> Path:
    """Return the GraphML output file path for the Aarhus walk network."""
    project_root = Path(__file__).resolve().parents[3]
    output_dir = project_root / "data" / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / "aarhus_walk_network.graphml"


def download_and_save_graph() -> Path:
    """Fetch the pedestrian graph for Aarhus and save it as GraphML.

    Returns:
        Path: The saved GraphML file location.
    """
    output_path = get_output_path()
    logger.info("Starting download of Aarhus pedestrian graph from OpenStreetMap...")

    try:
        graph = ox.graph_from_place("Aarhus, Denmark", network_type="walk")
    except TypeError as exc:
        logger.warning(
            "Direct geocoding of 'Aarhus, Denmark' did not return a polygon; retrying with 'Aarhus Municipality, Denmark'."
        )
        graph = ox.graph_from_place("Aarhus Municipality, Denmark", network_type="walk")

    node_count = len(graph.nodes)
    edge_count = len(graph.edges)
    logger.info("Fetched graph successfully: %s nodes, %s edges.", node_count, edge_count)

    ox.save_graphml(graph, output_path)
    logger.info("Saved Aarhus walk network to %s", output_path)

    return output_path


if __name__ == "__main__":
    download_and_save_graph()
