
import os
import requests
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# Waitlist Radar models
# -----------------------

class Treatment(BaseModel):
    """
    Represents a medical treatment/specialty that can be searched against.
    """
    code: str
    name: str


class SearchResult(BaseModel):
    """
    Represents a search result for a provider wait time query.
    """
    ods_code: str
    name: str
    distance_km: float
    median_weeks: float
    pct_over_52w: float
    cqc_overall: Optional[str] = None


class AlertRequest(BaseModel):
    """
    Represents an alert subscription request. In a real implementation
    this would be persisted to a database for later processing.
    """
    email: str
    postcode: str
    treatment: str
    threshold_weeks: float


# In-memory store for alert requests. This is a placeholder until a database is introduced.
ALERTS: List[AlertRequest] = []

# Static list of treatments for demonstration purposes. Replace with a database query in production.
STATIC_TREATMENTS: List[Treatment] = [
    Treatment(code="HIP", name="Hip Replacement"),
    Treatment(code="KNEE", name="Knee Replacement"),
    Treatment(code="CAT", name="Cataract Surgery"),
    Treatment(code="MRI", name="MRI Scan"),
    Treatment(code="ENT", name="ENT Consultation"),
]

# Static search results for demonstration purposes. In production, this should query a database
# with current waiting list information, CQC ratings and geospatial queries.
STATIC_RESULTS: List[SearchResult] = [
    SearchResult(
        ods_code="RJZ",
        name="Leeds General Infirmary",
        distance_km=2.1,
        median_weeks=42.0,
        pct_over_52w=0.10,
        cqc_overall="Good",
    ),
    SearchResult(
        ods_code="RWA",
        name="St James's University Hospital",
        distance_km=3.5,
        median_weeks=39.0,
        pct_over_52w=0.08,
        cqc_overall="Outstanding",
    ),
]

POSTCODES_IO_URL = "https://api.postcodes.io/postcodes/"
UKPN_FAULTS_API = "https://ukpowernetworks.opendatasoft.com/api/records/1.0/search/"

@app.post("/lookup")
async def lookup_postcode(postcode: str):
    res = requests.get(f"{POSTCODES_IO_URL}{postcode}")
    if res.status_code != 200:
        raise HTTPException(status_code=400, detail="Invalid postcode")

    data = res.json()
    lat = data["result"]["latitude"]
    lon = data["result"]["longitude"]

    params = {
        "dataset": "faults-and-interruptions",
        "geofilter.distance": f"{lat},{lon},1000",
        "rows": 100
    }
    faults_res = requests.get(UKPN_FAULTS_API, params=params)
    if faults_res.status_code != 200:
        raise HTTPException(status_code=500, detail="Error fetching UKPN data")

    fault_data = faults_res.json()["records"]
    fault_count = len(fault_data)

    if fault_count < 10:
        score = "Low Risk"
    elif fault_count < 30:
        score = "Moderate Risk"
    else:
        score = "High Risk"

    return {
        "postcode": postcode,
        "latitude": lat,
        "longitude": lon,
        "fault_count": fault_count,
        "risk_score": score
    }

# -----------------------------------------
# Waitlist Radar API endpoints
# -----------------------------------------

@app.get("/api/health")
async def health() -> dict:
    """
    Health-check endpoint used by the frontend to verify the API is reachable.
    """
    return {"ok": True}


@app.get("/api/treatments", response_model=List[Treatment])
async def get_treatments() -> List[Treatment]:
    """
    Returns a list of available treatments/specialties. This version returns
    a static list for demonstration but should be replaced with a database query.
    """
    return STATIC_TREATMENTS


@app.get("/api/search", response_model=List[SearchResult])
async def search(
    postcode: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    treatment: Optional[str] = None,
    radius_km: float = 50.0,
) -> List[SearchResult]:
    """
    Searches for providers within a given radius of a postcode or lat/lon and returns
    wait time information. This is a simplified version that returns static results
    and performs basic postcode geocoding using postcodes.io when latitude and longitude
    are not supplied.

    Parameters:
    - postcode: UK postcode. Required if lat/lon are not provided.
    - lat/lon: Geographic coordinates. If provided, postcode is optional.
    - treatment: Code of the treatment/specialty to search for. Optional in this demo.
    - radius_km: Search radius in kilometres. Currently unused in this demo.

    Returns:
    A list of providers with wait time metrics and CQC ratings.
    """

    # If latitude/longitude are missing but a postcode is provided, look up the coordinates.
    if (lat is None or lon is None) and postcode:
        res = requests.get(f"{POSTCODES_IO_URL}{postcode}")
        if res.status_code != 200:
            raise HTTPException(status_code=400, detail="Invalid postcode")
        data = res.json()
        lat = data["result"]["latitude"]
        lon = data["result"]["longitude"]

    # In a real implementation, you would perform a geospatial query here.
    # For the sake of this demo, simply return a subset of STATIC_RESULTS.
    # Optionally filter by treatment code if provided.
    results = STATIC_RESULTS
    if treatment:
        # This example does not have per-treatment data, so it returns the static list.
        # You could add logic here to filter based on treatment or adjust median_weeks.
        results = [r for r in STATIC_RESULTS]

    return results


@app.post("/api/alerts")
async def create_alert(alert: AlertRequest) -> dict:
    """
    Registers an alert for a given postcode and treatment. In production, this would persist
    the alert request to a database and trigger notifications when conditions are met.
    """
    ALERTS.append(alert)
    return {"status": "created", "alert": alert}
