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
async def get_sensor_data_clickhouse_query(
    auth_data: AuthRequest,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=1000),
):
    """
    Paginated sensor data via raw ClickHouse query (native protocol).
    Returns: { sensor_data: [...], total: int, offset: int, limit: int, status: str }
    """
    try:
        print("=" * 60)
        print("[1/6] Enter /current-sensor-data-clickhouse-query")
        print(f"[1/6] Auth: {auth_data}, offset={offset}, limit={limit}")

        # Authenticate
        print("[2/6] Authenticating...")
        is_auth = await is_authenticated(auth_data)
        print(f"[2/6] Auth result: {is_auth}")
        if not is_auth:
            return {
                "sensor_data": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "status": "unauthorized",
            }

        # Connect to ClickHouse (prefer native port 9000)
        clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", 8123))
        
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=clickhouse_port,
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_SENSOR_DATABASE", "datasnake"),
        )
        print(
            f"[3/6] Connected to ClickHouse {os.getenv('CLICKHOUSE_HOST','127.0.0.1')}:{clickhouse_port}"
        )

        # Get total count (separate fast count query)
        print("[4/6] Fetching total count...")
        total_res = client.query("SELECT COUNT(*) as total FROM sensor_data_processed")
        total = int(total_res.result_rows[0][0]) if total_res.result_rows else 0
        print(f"[4/6] Total rows: {total}")

        # Page query (ORDER BY for deterministic pagination)
        print("[5/6] Running paginated query...")
        sql = f"""
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
        # Use Arrow result and convert to Polars -> native dicts
        arrow_table = client.query_arrow(sql)
        if arrow_table is None:
            data = []
            print("[5/6] No rows returned (arrow_table is None).")
        else:
            pl_df = pl.from_arrow(arrow_table)
            # If any numpy/arrow scalars remain, to_dicts() will normalize into Python types
            data = pl_df.to_dicts()
            print(f"[5/6] Retrieved {len(data)} rows (converted to native types)")

        # Response
        print("[6/6] Returning response")
        print("=" * 60)
        return {
            "sensor_data": data,
            "total": total,
            "offset": offset,
            "limit": limit,
            "status": "success",
        }

    except Exception as e:
        logging.exception("Error in current-sensor-data-clickhouse-query")
        raise HTTPException(
            status_code=500, detail=f"Error fetching sensor data: {str(e)}"
        )


# @router.post("/current-sensor-data-clickhouse-query")
# async def get_sensor_data(auth_data: AuthRequest):
#     try:
#         is_auth = await is_authenticated(auth_data)
#         if not is_auth:
#             return {"sensor_data": [], "status": "unauthorized"}

#         # Fetch from ClickHouse
#         client = clickhouse_connect.get_client(
#             host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
#             port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
#             user=os.getenv("CLICKHOUSE_USER", "default"),
#             password=os.getenv("CLICKHOUSE_PASSWORD"),
#             database=os.getenv("CLICKHOUSE_SENSOR_DATABASE"),
#         )

#         result_arrow = client.query_arrow(
#             """
#             SELECT lat, lon, temp, humidity, country, state
#             FROM sensor_data_processed
#             LIMIT 5
#         """
#         )
#         # print("result_df:", result_df)
#         pl_df = pl.from_arrow(result_arrow)
#         data = pl_df.to_dicts()

#         return {"sensor_data": data, "status": "success"}

#     except Exception as e:
#         raise HTTPException(
#             status_code=500, detail=f"Error fetching sensor data: {str(e)}"
#         )
