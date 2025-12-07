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
async def get_sensor_data(
    auth_data: AuthRequest,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=1000),
):
    """
    Fetch paginated sensor data from ClickHouse.

    Parameters:
    - auth_data: Username and password for authentication
    - offset: Starting row (0-indexed, default 0)
    - limit: Rows to return (default 50, max 1000)

    Returns:
    - sensor_data: Array of sensor records
    - total: Total count of records in database
    - offset: Offset used in query
    - limit: Limit used in query
    - status: Success or error status
    """
    try:
        print("Inside API client get sensor data")
        print("Auth Data:", auth_data)

        # ✅ Authenticate
        response = await is_authenticated(auth_data)
        if not response:
            return {
                "sensor_data": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "message": "Authentication failed.",
                "status": "unauthorized",
            }

        # ✅ Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CLICKHOUSE_PORT", 9000)),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_SENSOR_DATABASE", "datasnake"),
        )

        # ✅ Get total count
        total_result = client.query(
            "SELECT COUNT(*) as total FROM sensor_data_processed"
        )
        total_count = total_result.result_rows[0][0] if total_result.result_rows else 0
        print(f"Total records in database: {total_count}")

        # ✅ Query with pagination
        query = f"""
            SELECT 
                device_id,
                lat,
                lon,
                temp,
                humidity,
                pressure,
                country,
                state,
                city,
                postal_code,
                timestamp
            FROM sensor_data_processed
            ORDER BY timestamp DESC
            LIMIT {limit} OFFSET {offset}
        """

        result = client.query(query)
        rows = result.result_rows
        columns = result.result_columns

        print(f"Query returned {len(rows)} rows")

        # ✅ Convert to Polars
        if rows:
            pl_df = pl.DataFrame(rows, schema=columns)
            data = pl_df.to_dicts()
        else:
            data = []

        return {
            "sensor_data": data,
            "total": int(total_count),
            "offset": offset,
            "limit": limit,
            "status": "success",
        }

    except Exception as e:
        logging.error(f"Error fetching sensor data: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error fetching sensor data: {str(e)}"
        )


# @router.post("/current-sensor-data")
# async def get_sensor_data(auth_data: AuthRequest):
#     try:
#         is_auth = await is_authenticated(auth_data)
#         if not is_auth:
#             return {"sensor_data": [], "status": "unauthorized"}

#         # Fetch from ClickHouse
#         client = clickhouse_connect.get_client(
#             host="127.0.0.1", password="", database="datasnake"
#         )
#         result = client.query(
#             """
#             SELECT lat, lon, temp, humidity, country, state
#             FROM sensor_data_processed
#             LIMIT 5
#         """
#         )
#         rows = result.result_rows
#         columns = result.result_columns

#         # Load into Polars
#         df = pl.DataFrame(rows, schema=columns)
#         print("came back to df", df)
#         # Register with DuckDB
#         con = duckdb.connect()
#         con.register("sensor_data", df.to_arrow())

#         # Run DuckDB SQL (e.g., local filtering or aggregations)
#         result_df = pl.from_arrow(
#             con.execute(
#                 """
#                 SELECT country, COUNT(*) as count, AVG(temp) as avg_temp
#                 FROM sensor_data
#                 GROUP BY country
#                 ORDER BY count DESC
#                 LIMIT 5
#             """
#             ).arrow()
#         )
#         print("aggregated:", result_df)

#         return {"sensor_data": df.to_dicts(), "status": "success"}

#     except Exception as e:
#         raise HTTPException(
#             status_code=500, detail=f"Error fetching sensor data: {str(e)}"
#         )
