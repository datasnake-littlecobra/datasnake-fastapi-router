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

        # ✅ Connect to ClickHouse via raw SQL (more reliable for pagination)
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CLICKHOUSE_PORT", 9000)),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_SENSOR_DATABASE", "datasnake"),
        )

        # ✅ Get total count first
        total_result = client.query(
            "SELECT COUNT(*) as total FROM sensor_data_processed"
        )
        total_count = total_result.result_rows[0][0] if total_result.result_rows else 0
        print(f"Total records in database: {total_count}")

        # ✅ Query with pagination and ordering
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
        columns = [col[0] for col in result.column_names]

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

        # ✅ Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CLICKHOUSE_PORT", 9000)),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_SENSOR_DATABASE", "datasnake"),
        )

        # ✅ Build WHERE clause dynamically
        where_clauses = []
        if country:
            where_clauses.append(f"country = '{country}'")
        if state:
            where_clauses.append(f"state = '{state}'")
        if temp_min is not None:
            where_clauses.append(f"temp >= {temp_min}")
        if temp_max is not None:
            where_clauses.append(f"temp <= {temp_max}")

        where_clause = " AND ".join(where_clauses)
        if where_clause:
            where_clause = f"WHERE {where_clause}"

        # ✅ Get filtered total count
        count_query = (
            f"SELECT COUNT(*) as total FROM sensor_data_processed {where_clause}"
        )
        total_result = client.query(count_query)
        total_count = total_result.result_rows[0][0] if total_result.result_rows else 0
        print(f"Filtered records: {total_count}")

        # ✅ Query with filters, pagination, and ordering
        data_query = f"""
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
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT {limit} OFFSET {offset}
        """

        result = client.query(data_query)
        rows = result.result_rows
        columns = [col[0] for col in result.column_names]

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
