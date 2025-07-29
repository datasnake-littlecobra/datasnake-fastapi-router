import clickhouse_connect
from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
import logging
import httpx

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


@router.post("/current-sensor-data")
async def get_sensor_data(auth_data: AuthRequest):
    try:
        print("inside api client get sensor data :")
        print("inside api client get sensor data :", auth_data)
        response = await is_authenticated(auth_data)
        # session = get_cassandra_session()
        
        # Establish a connection
        # user="your_user",
        client = clickhouse_connect.get_client(
            host="127.0.0.1",
            password="",
            database="datasnake",
        )

        # Execute a query and fetch results
        query = """
        SELECT lat, lon, temp, humidity, country, state 
        FROM sensor_data_processed 
        LIMIT 5
        """
        result = client.query(query)

        # Access data
        rows = result.result_rows  # Get data as a list of tuples
        columns = result.column_names  # Get column names

        # rows = session.execute(query)

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
