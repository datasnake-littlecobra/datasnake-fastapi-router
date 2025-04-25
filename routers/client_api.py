from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
from utils.security import decode_access_token
from fastapi.security import OAuth2PasswordBearer
from utils.security import verify_token
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import httpx
import sqlite3
import logging

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")
cassandra_user = ""
cassandra_password = ""

# def get_current_user(authorization: str = Header(None)):
#     if not authorization or not authorization.startswith("Bearer "):
#         raise HTTPException(status_code=401, detail="Invalid or missing token")

#     token = authorization.split(" ")[1]
#     payload = decode_access_token(token)

#     if not payload:
#         raise HTTPException(status_code=401, detail="Invalid token")

#     return payload["sub"]


def get_current_user(token: str = Depends(oauth2_scheme)):
    return verify_token(token)


# Initialize Cassandra connection
def get_cassandra_session():
    auth_provider = PlainTextAuthProvider(
        cassandra_user, cassandra_password
    )  # Update credentials
    cluster = Cluster(
        ["127.0.0.1"], auth_provider=auth_provider
    )  # Update with actual IP
    session = cluster.connect()
    session.set_keyspace("datasnake")
    return session


@router.get("/api/client/data")
def get_client_data(username: str = Depends(get_current_user)):
    return {"message": f"Hello {username}, here is your data!"}


CLOUDFLARE_WORKER_URL_LOGIN = (
    "https://d1-worker-production.rustchain64.workers.dev/api/login"
)

CLOUDFLARE_WORKER_URL_GET_USERS = (
    "https://d1-worker-production.rustchain64.workers.dev/api/users"
)


# Request Model
class AuthRequest(BaseModel):
    username: str
    password: str


# $ curl -X POST http://localhost:8000/api/cloudflare-login   -H "Content-Type: application/json"   -d '{"username": "prabhakar_10sharma", "password": "psPS_datasnake"}'
@router.post("/api/cloudflare-login")
async def login(auth_data: AuthRequest):
    print("inside cloudflare login auth:", auth_data)
    logging.info(f"inside cloudflare login auth: {auth_data}")
    async with httpx.AsyncClient() as client:
        print("going to call client")
        response = await client.post(CLOUDFLARE_WORKER_URL_LOGIN, json=auth_data.dict())
        print("after calling client")

    if response.status_code != 200:
        print("did i get here !200 response???")
        logging.info("did i get here !200 response???")
        raise HTTPException(
            status_code=response.status_code, detail="Authentication failed"
        )

    data = response.json()

    logging.info(f"200 response with {data}")
    print("200 response with", data)
    return {
        "status": response.status_code,
        "token": data.get("user").get("uuid"),
        "role": data.get("user").get("role"),
        "message": data.get("message"),
        "username": data.get("user").get("username"),
    }


async def is_authenticated(request: AuthRequest):
    print("inside is_authenticated:", request)
    logging.info(f"inside cloudflare login auth: {request}")
    print(f"inside cloudflare login auth: {request}")
    async with httpx.AsyncClient() as client:
        print("going to call client")
        response = await client.post(CLOUDFLARE_WORKER_URL_LOGIN, json=request.dict())
        print("after calling client", response.json())

    if response.status_code != 200:
        print("did i get here !200 response???")
        logging.info("did i get here !200 response???")
        raise HTTPException(
            status_code=response.status_code, detail="Authentication failed"
        )

    logging.info(f"status code - {response.status_code}")
    print("status code: ", response.status_code)
    return {
        "status": response.status_code,
        "username": response.json().get("user").get("username"),
    }


async def check_auth(request: Request):
    """Checks if the request has a valid JWT token from Cloudflare login."""
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")

    token = auth_header.split("Bearer ")[1]

    # Simply forward the token to Cloudflare Worker to verify it
    async with httpx.AsyncClient() as client:
        response = await client.post(CLOUDFLARE_WORKER_URL_LOGIN, json={"token": token})

    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="Authentication failed")

    return response.json()


class LoginUser(BaseModel):
    username: str


@router.get("/api/getuser")
async def get_user(username: str = Query(..., description="Username to fetch from DB")):
    print("inside get_user:", username)

    return "single-user"


@router.get("/api/getusers")
async def get_users():
    async with httpx.AsyncClient() as client:
        response = await client.post(CLOUDFLARE_WORKER_URL_GET_USERS)

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code, detail="Failed to fetch user"
        )
    print("all user:", response.json())
    return [dict(user) for user in response.json()]
    # return response.json()


# curl -X GET http://localhost:8000/api/client/sensor-data \
#      -H "Authorization: Bearer YOUR_JWT_TOKEN"
@router.get("/api/client/sensor-data")
def get_sensor_data(auth_response: dict = Depends(is_authenticated)):
    try:
        # print(f"data returned from is_authenticated: {auth_response}")
        # logging.info(f"data returned from is_authenticated: {auth_response}")
        session = get_cassandra_session()
        query = """
        SELECT lat, lon, temp, humidity, country, state 
        FROM sensor_data_processed 
        LIMIT 5
        """
        rows = session.execute(query)

        data = [
            {
                "lat": row.lat,
                "lon": row.lon,
                "temp": row.temp,
                "humidity": row.humidity,
                "country": row.country,
                "state": row.state,
            }
            for row in rows
        ]

        return {"sensor_data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# request=>lat/lon ( with no time duration )
# response ( past month )
@router.get("/api/client/sensor-data-by-lat-lon")
def get_sensor_data(
    lat: str, lon: str, auth_response: dict = Depends(is_authenticated)
):
    try:
        # print(f"data returned from is_authenticated: {auth_response}")
        # logging.info(f"data returned from is_authenticated: {auth_response}")
        session = get_cassandra_session()
        query = """
        SELECT lat, lon, temp, humidity, country, state 
        FROM sensor_data_processed 
        WHERE lat = %s and lon = %s
        LIMIT 5
        """
        rows = session.execute(query)

        data = [
            {
                "lat": row.lat,
                "lon": row.lon,
                "temp": row.temp,
                "humidity": row.humidity,
                "country": row.country,
                "state": row.state,
            }
            for row in rows
        ]

        if not data:
            raise HTTPException(
                status_code=404,
                detail="No sensor data found for the given coordinates.",
            )

        return {"sensor_data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/client/sensor-data-by-lat-lon-range")
def get_sensor_data_by_range(
    min_lat: str,
    max_lat: str,
    min_lon: str,
    max_lon: str,
    auth_response: dict = Depends(is_authenticated),
):
    try:
        session = get_cassandra_session()
        query = """
        SELECT lat, lon, temp, humidity, country, state 
        FROM sensor_data_processed 
        WHERE lat >= %s AND lat <= %s 
        AND lon >= %s AND lon <= %s 
        LIMIT 10
        """
        rows = session.execute(query, (min_lat, max_lat, min_lon, max_lon))

        data = [
            {
                "lat": row.lat,
                "lon": row.lon,
                "temp": row.temp,
                "humidity": row.humidity,
                "country": row.country,
                "state": row.state,
            }
            for row in rows
        ]

        if not data:
            raise HTTPException(
                status_code=404, detail="No sensor data found in the given range."
            )

        return {"sensor_data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# request=>lat/lon/start_time/end_time

# request=>lat/lon/days


# request=>start_time/end_time

# request=>country/state/city

# request=>country/state/city/start_time/end_time

# request=>country/state/city/days

# request=>


# def get_sensor_data(country: str, state: str, limit: int = 10):
#     try:
#         query = """
#         SELECT lat, lon, temp, humidity, country, state
#         FROM sensor_data_processed
#         WHERE country = %s AND state = %s
#         LIMIT %s
#         """
#         rows = session.execute(query, (country, state, limit))

#         data = [{"lat": row.lat, "lon": row.lon, "temp": row.temp, "humidity": row.humidity,
#                  "country": row.country, "state": row.state} for row in rows]

#         return {"sensor_data": data}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# GET /sensor-averages
# GET /sensor-averages?country=US&state=CA&aggregation=daily
# Logic:
# Compute mean values for temperature, humidity, etc., using Polars for efficient aggregation.
# If aggregation is provided, return averages at that granularity.
# If no date range is provided, return last 7 days of data by default.


# GET /sensor-extremes
# GET /sensor-extremes?postal_code=94103&metric=temperature
# Logic:
# Find max/min values within the given range for the specified metric.
# Optimize queries with Cassandra’s partition key strategy.


# GET /sensor-trends
# GET /sensor-trends?state=NY&metric=humidity&window=30d
# Logic:
# Use moving averages or anomaly detection (Z-score, seasonal decomposition) to highlight trends.
# This helps detect heatwaves, cold snaps, or humidity spikes.
