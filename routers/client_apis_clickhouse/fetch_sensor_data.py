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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def is_authenticated(request: AuthRequest):
    """Authenticate user via Cloudflare Worker"""
    print("inside is_authenticated:", request)
    logger.info(f"inside cloudflare login auth: {request}")

    try:
        async with httpx.AsyncClient() as client:
            print("going to call cloudflare client inside clickhouse api route")
            response = await client.post(
                CLOUDFLARE_WORKER_URL_LOGIN, json=request.dict()
            )
            print("after calling client", response.json())

        if response.status_code != 200:
            print("did i get here !200 response???")
            logger.info("did i get here !200 response???")
            raise HTTPException(
                status_code=response.status_code, detail="Authentication failed"
            )

        logger.info(f"status code - {response.status_code}")
        print("status code: ", response.status_code)
        return {
            "status": response.status_code,
            "username": response.json().get("user").get("username"),
        }
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(status_code=401, detail="Authentication failed")


@router.post("/current-sensor-data")
async def get_sensor_data(
    auth_data: AuthRequest,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),  # Max 100 rows per request
):
    """
    Fetch paginated sensor data from ClickHouse.

    Parameters:
    - auth_data: Username and password for authentication
    - offset: Starting row (0-indexed, default 0)
    - limit: Rows to return (default 50, max 100)

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

        # ✅ Connect to ClickHouse via Ibis
        con = ibis.clickhouse.connect(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_SENSOR_DATABASE", "datasnake"),
        )

        # ✅ Get total count first (for client pagination)
        table = con.table("sensor_data_processed")
        total_count = table.count().execute()
        print(f"Total records in database: {total_count}")

        # ✅ Define Ibis table and query with pagination + ordering
        query = (
            table[
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
                    "timestamp",
                ]
            ]
            .order_by(ibis.desc("timestamp"))  # Latest first
            .limit(limit)
            .offset(offset)
        )

        # ✅ Execute query (this returns a pandas DataFrame)
        result_df = query.execute()
        print(f"Query returned {len(result_df)} rows")

        # ✅ Convert to Polars for consistency
        pl_df = pl.DataFrame(result_df)

        # ✅ Convert to JSON-serializable dict list
        data = pl_df.to_dicts()

        return {
            "sensor_data": data,
            "total": int(total_count),
            "offset": offset,
            "limit": limit,
            "status": "success",
        }

    except Exception as e:
        logger.error(f"Error fetching sensor data: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error fetching sensor data: {str(e)}"
        )


@router.post("/current-sensor-data-filtered")
async def get_sensor_data_filtered(
    auth_data: AuthRequest,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    country: str = Query(None),
    state: str = Query(None),
    temp_min: float = Query(None),
    temp_max: float = Query(None),
):
    """
    Fetch paginated sensor data with optional server-side filtering.

    Parameters:
    - auth_data: Username and password for authentication
    - offset: Starting row (0-indexed)
    - limit: Rows to return (default 50, max 100)
    - country: Filter by country code (optional)
    - state: Filter by state name (optional)
    - temp_min: Minimum temperature (optional)
    - temp_max: Maximum temperature (optional)

    Returns:
    - sensor_data: Filtered array of sensor records
    - total: Total count matching filters
    - offset, limit, status
    """
    try:
        print("Inside API client get sensor data (filtered)")
        print("Auth Data:", auth_data)

        # ✅ Authenticate
        response = await is_authenticated(auth_data)
        if not response:
            return {
                "sensor_data": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "status": "unauthorized",
            }

        # ✅ Connect to ClickHouse via Ibis
        con = ibis.clickhouse.connect(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_SENSOR_DATABASE", "datasnake"),
        )

        # ✅ Build query with filters
        table = con.table("sensor_data_processed")

        # Apply filters progressively
        if country:
            table = table.filter(table.country == country)
        if state:
            table = table.filter(table.state == state)
        if temp_min is not None:
            table = table.filter(table.temp >= temp_min)
        if temp_max is not None:
            table = table.filter(table.temp <= temp_max)

        # ✅ Get filtered total count
        total_count = table.count().execute()
        print(f"Filtered records: {total_count}")

        # ✅ Apply pagination and ordering
        query = (
            table[
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
                    "timestamp",
                ]
            ]
            .order_by(ibis.desc("timestamp"))
            .limit(limit)
            .offset(offset)
        )

        # ✅ Execute query
        result_df = query.execute()
        pl_df = pl.DataFrame(result_df)
        data = pl_df.to_dicts()

        return {
            "sensor_data": data,
            "total": int(total_count),
            "offset": offset,
            "limit": limit,
            "filters": {
                "country": country,
                "state": state,
                "temp_min": temp_min,
                "temp_max": temp_max,
            },
            "status": "success",
        }

    except Exception as e:
        logger.error(f"Error fetching filtered sensor data: {str(e)}")
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
