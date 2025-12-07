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
        print("=" * 60)
        print("[1/7] Inside API client get sensor data")
        print(f"[1/7] Auth Data: {auth_data}")
        print(f"[1/7] Query params - offset: {offset}, limit: {limit}")

        # ✅ Authenticate
        print("[2/7] Starting authentication...")
        response = await is_authenticated(auth_data)
        print(f"[2/7] Authentication response: {response}")
        
        if not response:
            print("[2/7] Authentication failed, returning early")
            return {
                "sensor_data": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "message": "Authentication failed.",
                "status": "unauthorized",
            }
        print("[2/7] ✅ Authentication successful")

        # ✅ Connect to ClickHouse
        print("[3/7] Connecting to ClickHouse...")
        clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", 9000))
        print(f"[3/7] Host: {os.getenv('CLICKHOUSE_HOST', '127.0.0.1')}, Port: {clickhouse_port}")
        
        # Use native protocol (9000) for better streaming support, not HTTP (8123)
        if clickhouse_port == 8123:
            print("[3/7] ⚠️  WARNING: Port 8123 is HTTP interface, switching to native protocol (9000)")
            clickhouse_port = 9000
        
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=clickhouse_port,
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_SENSOR_DATABASE", "datasnake"),
        )
        print("[3/7] ✅ ClickHouse client connected")

        # ✅ Get total count
        print("[4/7] Fetching total record count...")
        total_result = client.query(
            "SELECT COUNT(*) as total FROM sensor_data_processed"
        )
        total_count = total_result.result_rows[0][0] if total_result.result_rows else 0
        print(f"[4/7] ✅ Total records in database: {total_count}")

        # ✅ Query with pagination
        print(f"[5/7] Building pagination query with LIMIT {limit} OFFSET {offset}...")
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
        print(f"[5/7] Query built successfully")

        print("[6/7] Executing query...")
        result = client.query(query)
        print("[6/7] Query executed, fetching result as list...")
        # Use result_as_list() to properly materialize the entire result set
        rows = result.result_as_list()
        columns = result.result_columns
        print(f"[6/7] ✅ Query returned {len(rows)} rows, columns: {columns}")

        # ✅ Convert to Polars
        print("[7/7] Converting to Polars DataFrame...")
        if rows:
            print(f"[7/7] Creating Polars DataFrame with {len(rows)} rows...")
            pl_df = pl.DataFrame(rows, schema=columns)
            print(f"[7/7] Converting DataFrame to dicts...")
            data = pl_df.to_dicts()
            print(f"[7/7] ✅ Conversion complete, {len(data)} records ready")
        else:
            print("[7/7] No rows returned, empty data list")
            data = []

        print("[7/7] ✅ Returning response")
        print("=" * 60)
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
