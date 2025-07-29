from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
from utils.cassandra_session import get_cassandra_session
import httpx
import sqlite3
import logging

router = APIRouter()


class AuthRequest(BaseModel):
    username: str
    password: str


CLOUDFLARE_WORKER_URL_LOGIN = (
    "https://d1-worker-production.rustchain64.workers.dev/api/login"
)


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


@router.post("/current-sensor-data-cassandra")
async def get_sensor_data(auth_data: AuthRequest):
    try:
        print("inside api client get sensor data :")
        print("inside api client get sensor data :", auth_data)
        response = await is_authenticated(auth_data)
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


class SensorDataLatLon(BaseModel):
    username: str
    password: str
    lat: float
    lon: float


@router.post("/current-sensor-data-by-lat-lon")
async def get_sensor_data_by_lat_lon(auth_data: SensorDataLatLon):
    try:
        print("inside api client get sensor data by LAT LON:")
        print("inside api client get sensor data by LAT LON:", auth_data)
        credentials = {"username": auth_data.username, "password": auth_data.password}
        response = await is_authenticated(AuthRequest(**credentials))
        print("get_sensor_data_by_lat_lon AUTHENTICATED!!!")
        session = get_cassandra_session()
        query = """
        SELECT lat, lon, temp, humidity, country, state 
        FROM sensor_data_by_lat_lon 
        WHERE lat = %s and lon = %s 
        LIMIT 5
        """
        rows = session.execute(query, (auth_data.lat, auth_data.lon))

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


# @router.post("/current-sensor-data-by-lat-lon-range")


class SensorDataCountry(BaseModel):
    country: str
    lat: float
    lon: float


@router.post("/current-sensor-data-by-country")
async def get_sensor_data_by_country(auth_data: SensorDataCountry):
    try:
        print("inside api client get sensor data by country:")
        print("inside api client get sensor data by country:", auth_data)
        credentials = {"username": auth_data.username, "password": auth_data.password}
        response = await is_authenticated(AuthRequest(**credentials))
        print("get_sensor_data_by_lat_lon AUTHENTICATED!!!")
        session = get_cassandra_session()
        query = """
        SELECT lat, lon, temp, humidity, country, state 
        FROM sensor_data_processed 
        WHERE country = %s 
        LIMIT 5
        """
        rows = session.execute(query, (auth_data.lat, auth_data.lon))

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
