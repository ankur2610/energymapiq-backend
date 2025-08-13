
import os
import requests
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import psycopg2  # used to query the wait_metrics table for real wait times
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

# New model for trend information
class WaitTrend(BaseModel):
    """
    Represents a wait time trend for a specific provider and treatment.
    periods: List of time periods (e.g. months) in ISO 8601 format
    median_weeks: Corresponding median wait weeks for each period
    """
    ods_code: str
    treatment: str
    periods: list[str]
    median_weeks: list[float]

# Static trending data for demonstration purposes. In a production system this would be loaded
# from official NHS datasets (e.g. RTT or WLMDS) and refreshed regularly.
STATIC_TRENDS: list[WaitTrend] = [
    WaitTrend(
        ods_code="RJZ",
        treatment="HIP",
        periods=["2025-03", "2025-04", "2025-05", "2025-06"],
        median_weeks=[43.0, 42.0, 40.5, 39.0],
    ),
    WaitTrend(
        ods_code="RJZ",
        treatment="KNEE",
        periods=["2025-03", "2025-04", "2025-05", "2025-06"],
        median_weeks=[45.0, 44.0, 43.5, 43.0],
    ),
    WaitTrend(
        ods_code="RWA",
        treatment="HIP",
        periods=["2025-03", "2025-04", "2025-05", "2025-06"],
        median_weeks=[41.0, 40.0, 38.5, 38.0],
    ),
    WaitTrend(
        ods_code="RWA",
        treatment="KNEE",
        periods=["2025-03", "2025-04", "2025-05", "2025-06"],
        median_weeks=[42.0, 41.5, 41.0, 40.0],
    ),
]



POSTCODES_IO_URL = "https://api.postcodes.io/postcodes/"
UKPN_FAULTS_API = "https://ukpowernetworks.opendatasoft.com/api/records/1.0/search/"

# ---------------------------------------------------------------------------
# Database helpers
#
# The WLMDS loader script populates a `wait_metrics` table with columns:
#   ods_code, treatment_code, period_date, median_weeks, pct_over_18w, pct_over_52w
# These helpers provide a simple way to query the latest waiting time metrics
# without keeping a persistent connection open. In production you may want to
# use an async driver like asyncpg and connection pooling for efficiency.


def get_db_connection():
    """Return a new psycopg2 connection using the DATABASE_URL env variable."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable is not configured")
    return psycopg2.connect(db_url)


def fetch_latest_wait_metrics(treatment_code: str):
    """
    Fetch a list of tuples (ods_code, median_weeks, pct_over_18w, pct_over_52w)
    for the most recent period_date for the given treatment_code. Returns an
    empty list if no records exist.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Find the most recent period for this treatment
            cur.execute(
                """
                SELECT MAX(period_date)
                FROM wait_metrics
                WHERE treatment_code = %s
                """,
                (treatment_code,),
            )
            latest = cur.fetchone()[0]
            if not latest:
                return []
            cur.execute(
                """
                SELECT ods_code, median_weeks, pct_over_18w, pct_over_52w
                FROM wait_metrics
                WHERE treatment_code = %s AND period_date = %s
                ORDER BY median_weeks ASC
                """,
                (treatment_code, latest),
            )
            return cur.fetchall()
    finally:
        conn.close()


# Basic provider metadata. For demonstration this includes only a couple of hospitals
# used in the static sample. In a production system you'd populate this table
# from an authoritative source such as the NHS Organization Data Service (ODS) or
# the NHS Website Content API and include lat/lon for geospatial queries.
PROVIDER_INFO = {
    "RJZ": {
        "name": "Leeds General Infirmary",
        "lat": 53.8014,
        "lon": -1.5584,
    },
    "RWA": {
        "name": "St James's University Hospital",
        "lat": 53.8067,
        "lon": -1.5280,
    },
}



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

           # ---------------------------------------------------------------------
        # Fetch real wait time data if a treatment code has been specified and
        # the database is configured. If DATABASE_URL is missing or the query
        # returns no results, fall back to STATIC_RESULTS. This allows the
        # frontend to continue functioning with demo data while the ETL runs
        # asynchronously to populate the wait_metrics table.
        if treatment:
            try:
                records = fetch_latest_wait_metrics(treatment.upper())
            except Exception:
                records = []
            if records:
                # Build SearchResult objects from DB data. Distance is not
                # calculated in this simple example; for accurate distances
                # you'd join on provider coordinates and compute haversine.
                enriched: list[SearchResult] = []
                for ods_code, median_weeks, _, pct_over_52w in records:
                    info = PROVIDER_INFO.get(ods_code, {})
                    name = info.get("name", ods_code)
                    # Use provider coordinates if available to compute distance
                    if info.get("lat") and info.get("lon") and lat is not None and lon is not None:
                        # Haversine formula for distance in km
                        from math import radians, cos, sin, sqrt, atan2

                        R = 6371.0
                        lat1 = radians(lat)
                        lon1 = radians(lon)
                        lat2 = radians(info["lat"])
                        lon2 = radians(info["lon"])
                        dlat = lat2 - lat1
                        dlon = lon2 - lon1
                        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
                        c = 2 * atan2(sqrt(a), sqrt(1 - a))
                        distance = R * c
                    else:
                        distance = 0.0
                    enriched.append(
                        SearchResult(
                            ods_code=ods_code,
                            name=name,
                            distance_km=round(distance, 1),
                            median_weeks=median_weeks,
                            pct_over_52w=pct_over_52w,
                            cqc_overall=None,
                        )
                    )
                return enriched
        # If no treatment specified or DB query yielded nothing, return static sample data
        return STATIC_RESULTS


@app.post("/api/alerts")
async def create_alert(alert: AlertRequest) -> dict:
    """
    Registers an alert for a given postcode and treatment. In production, this would persist
    the alert request to a database and trigger notifications when conditions are met.
    """
    ALERTS.append(alert)
    return {"status": "created", "alert": alert}

# New endpoint to retrieve waiting time trends for a specific provider and treatment
@app.get("/api/trends", response_model=List[WaitTrend])
async def get_trends(ods_code: str, treatment: str) -> List[WaitTrend]:
    """
    Returns wait time trends for a given provider and treatment. The trend is
    computed from all available `wait_metrics` records in the database. If
    no records exist, returns the static demo trend data.
    """
    try:
        conn = get_db_connection()
    except Exception:
        conn = None
    trends: list[WaitTrend] = []
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT period_date, median_weeks
                    FROM wait_metrics
                    WHERE ods_code = %s AND treatment_code = %s
                    ORDER BY period_date ASC
                    """,
                    (ods_code, treatment),
                )
                rows = cur.fetchall()
                if rows:
                    periods = [r[0].strftime("%Y-%m") for r in rows]
                    medians = [r[1] for r in rows]
                    trends.append(
                        WaitTrend(
                            ods_code=ods_code,
                            treatment=treatment,
                            periods=periods,
                            median_weeks=medians,
                        )
                    )
        finally:
            conn.close()
    if trends:
        return trends
    # fall back to static data for demo
    return [trend for trend in STATIC_TRENDS if trend.ods_code == ods_code and trend.treatment == treatment]
