"""
JogAir FastAPI Backend
Exposes route calculation as a REST API endpoint.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import osmnx as ox

from src.collectors.pipeline.temporal_routing_orchestrator import TemporalRoutingOrchestrator

app = FastAPI(title="JogAir API")

# Mount static files
app.mount("/static", StaticFiles(directory="app"), name="static")

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
    request_time: str = None

def nodes_to_coords(graph, node_ids):
    """Convert list of node IDs to list of [lat, lon] coordinates."""
    coords = []
    for node in node_ids:
        data = graph.nodes[node]
        coords.append([data["y"], data["x"]])
    return coords

@app.get("/")
def serve_frontend():
    return FileResponse("app/index.html")

@app.post("/routes")
def get_routes(request: RouteRequest):
    try:
        # Parse request_time if provided, otherwise use current time
        if request.request_time:
            request_datetime = datetime.fromisoformat(request.request_time)
        else:
            request_datetime = datetime.now()
        
        result = orchestrator.calculate_temporal_routes(
            start_latitude=request.start_lat,
            start_longitude=request.start_lon,
            end_latitude=request.end_lat,
            end_longitude=request.end_lon,
            request_datetime=request_datetime
        )

        shortest_coords = nodes_to_coords(
            orchestrator.graph,
            result.shortest_distance_route.node_ids
        )
        clean_coords = nodes_to_coords(
            orchestrator.graph,
            result.clean_air_route.node_ids
        )

        # Calculate exposure scores
        shortest_exposure = result.shortest_distance_route.distance_m * result.pollution_value
        clean_exposure = result.clean_air_route.distance_m * result.pollution_value

        return {
            "shortest": {
                "coordinates": shortest_coords,
                "distance_m": result.shortest_distance_route.distance_m,
                "exposure": shortest_exposure,
            },
            "clean_air": {
                "coordinates": clean_coords,
                "distance_m": result.clean_air_route.distance_m,
                "exposure": clean_exposure,
            },
            "pollution_value": result.pollution_value,
            "pollution_unit": result.pollution_unit,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok"}