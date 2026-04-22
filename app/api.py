"""
JogAir FastAPI Backend
Exposes route calculation as a REST API endpoint.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import osmnx as ox

from src.collectors.pipeline.temporal_routing_orchestrator import TemporalRoutingOrchestrator

app = FastAPI(title="JogAir API")

# Allow frontend to talk to backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load orchestrator once at startup
orchestrator = TemporalRoutingOrchestrator(data_source="live")

class RouteRequest(BaseModel):
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float

def nodes_to_coords(graph, node_ids):
    """Convert list of node IDs to list of [lat, lon] coordinates."""
    coords = []
    for node in node_ids:
        data = graph.nodes[node]
        coords.append([data["y"], data["x"]])
    return coords

@app.post("/routes")
def get_routes(request: RouteRequest):
    try:
        result = orchestrator.calculate_temporal_routes(
            start_latitude=request.start_lat,
            start_longitude=request.start_lon,
            end_latitude=request.end_lat,
            end_longitude=request.end_lon,
            request_datetime=datetime.now()
        )

        shortest_coords = nodes_to_coords(
            orchestrator.graph,
            result.shortest_distance_route.node_ids
        )
        clean_coords = nodes_to_coords(
            orchestrator.graph,
            result.clean_air_route.node_ids
        )

        return {
            "shortest": {
                "coordinates": shortest_coords,
                "distance_m": result.shortest_distance_route.distance_m,
            },
            "clean_air": {
                "coordinates": clean_coords,
                "distance_m": result.clean_air_route.distance_m,
            },
            "pollution_value": result.pollution_value,
            "pollution_unit": result.pollution_unit,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok"}