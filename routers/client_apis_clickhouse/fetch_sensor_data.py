import clickhouse_connect
from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
import logging
import httpx
import polars as pl
import duckdb
import ibis
import os

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
        print("going to call cloudflare client inside clickhouse api route")
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
        print("Inside API client get sensor data")
        print("Auth Data:", auth_data)

        # ✅ Authenticate
        response = await is_authenticated(auth_data)
        if not response:
            return {
                "sensor_data": [],
                "message": "Authentication failed.",
                "status": "unauthorized",
            }

        # ✅ Connect to ClickHouse via Ibis
        con = ibis.clickhouse.connect(
            host="127.0.0.1",
            port=8123,
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database=os.getenv("CLICKHOUSE_SENSOR_DATABASE"),
        )

        # ✅ Define Ibis table and query
        table = con.table("sensor_data_processed")
        query = table[
            [
                "device_id",
                "lat",
                "lon",
                "temp",
                "humidity",
                "pressure",
                "country",
                "state",
                "city",
                "postal_code",
            ]
        ].limit(50)

        # ✅ Execute query (this returns a pandas DataFrame)
        result_df = query.execute()

        # ✅ Convert to Polars for consistency (optional)
        pl_df = pl.DataFrame(result_df)

        # ✅ Convert to JSON-serializable dict list
        data = pl_df.to_dicts()

        return {"sensor_data": data, "status": "success"}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching sensor data: {str(e)}"
        )


@router.post("/current-sensor-data-clickhouse-query")
async def get_sensor_data(auth_data: AuthRequest):
    try:
        is_auth = await is_authenticated(auth_data)
        if not is_auth:
            return {"sensor_data": [], "status": "unauthorized"}

        # Fetch from ClickHouse
        client = clickhouse_connect.get_client(
            host="127.0.0.1", password=os.getenv("CLICKHOUSE_PASSWORD"), database=os.getenv("CLICKHOUSE_SENSOR_DATABASE")
        )
        result = client.query(
            """
            SELECT lat, lon, temp, humidity, country, state
            FROM sensor_data_processed
            LIMIT 5
        """
        )
        rows = result.result_rows
        columns = result.result_columns

        # Load into Polars
        df = pl.DataFrame(rows, schema=columns)
        print("came back to df", df)
        # Register with DuckDB
        con = duckdb.connect()
        con.register("sensor_data", df.to_arrow())

        # Run DuckDB SQL (e.g., local filtering or aggregations)
        result_df = pl.from_arrow(
            con.execute(
                """
                SELECT country, COUNT(*) as count, AVG(temp) as avg_temp
                FROM sensor_data
                GROUP BY country
                ORDER BY count DESC
                LIMIT 5
            """
            ).arrow()
        )
        print("aggregated:", result_df)

        return {"sensor_data": df.to_dicts(), "status": "success"}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching sensor data: {str(e)}"
        )
