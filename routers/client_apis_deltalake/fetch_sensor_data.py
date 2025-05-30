import daft.delta_lake
from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
from utils.cassandra_session import get_cassandra_session
import httpx
import logging
import polars as pl
import duckdb
import daft
from daft.sql import SQLCatalog
from deltalake import DeltaTable

router = APIRouter()

sensor_delta_s3_bucket_raw_prod = "datasnake"
sensor_delta_s3_key_raw_prod = "deltalake_sensor_data_processed"
read_paths = {
    "datasnake_sensor_data_processed_deltalake_local": "/home/resources/deltalake-sensor-data-processed",
    "datasnake_sensor_data_processed_deltalake_remote": f"s3://{sensor_delta_s3_bucket_raw_prod}/{sensor_delta_s3_key_raw_prod}/",
}
hostname = "sjc1.vultrobjects.com"
s3_config = {
    "AWS_ACCESS_KEY_ID": "",
    "AWS_SECRET_ACCESS_KEY": "",
    "AWS_ENDPOINT_URL": "https://sjc1.vultrobjects.com",
}
storage_options = {
    "AWS_ACCESS_KEY_ID": "",
    "AWS_SECRET_ACCESS_KEY": "",
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_S3_USE_INSTANCE_METADATA": "false",
}
duckdb_storage_options = {
    "s3_access_key_id": "",
    "s3_secret_access_key": "",
    "s3_endpoint": f"https://{hostname}",
    "s3_region": "us-east-1",
    "s3_url_style": "path",
}
dask_storage_options = {
    "AWS_ENDPOINT_URL": "https://sjc1.vultrobjects.com",
    "AWS_ACCESS_KEY_ID": "",
    "AWS_SECRET_ACCESS_KEY": "",
}

from daft.io import IOConfig, S3Config

# io_config = IOConfig(
#     s3={
#         "AWS_ENDPOINT_URL": "https://sjc1.vultrobjects.com",
#         "AWS_REGION": "us-east-1",
#         "AWS_ACCESS_KEY_ID": "",
#         "AWS_SECRET_ACCESS_KEY": "",
#     }
# )

s3_config = S3Config(
    key_id="",
    access_key="",
    region_name="us-east-1",
    endpoint_url="https://sjc1.vultrobjects.com",
)
io_config1 = IOConfig(s3_config)


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
        dt = DeltaTable(
            read_paths["datasnake_sensor_data_processed_deltalake_remote"],
            storage_options={
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
                "AWS_ENDPOINT": "https://sjc1.vultrobjects.com",
            },
        )

        # dt = DeltaTable(read_paths["datasnake_sensor_data_processed_deltalake_local"])

        con = duckdb.connect()
        duckdb.sql(
            """
            INSTALL delta;
            LOAD delta;
            """
        )

        # Convert to Polars DataFrame
        print("TILL HERE 2")
        pl_df = pl.from_arrow(dt.to_pyarrow_table())
        # register polars df to duckdb table
        con.register("target_table", pl_df)

        output_df = con.execute(
            """
            SELECT lat, lon, temp, humidity, country, state 
            FROM target_table 
            LIMIT 10
            """
        ).pl()

        print("printing output df")
        print(output_df.head())

        data = output_df.select(
            ["lat", "lon", "temp", "humidity", "country", "state"]
        ).to_dicts()
        return {"sensor_data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/current-sensor-data-duckdb-s3")
async def get_sensor_data(auth_data: AuthRequest):
    try:
        print("inside api client get sensor data :")
        print("inside api client get sensor data :", auth_data)
        response = await is_authenticated(auth_data)

        con = duckdb.connect()
        con.sql("INSTALL delta; LOAD delta;")
        con.sql("INSTALL httpfs; LOAD httpfs;")

        # Dynamically set DuckDB's S3 parameters
        # for key, value in duckdb_storage_options.items():
        #     con.sql(f"SET {key} = '{value}';")

        # con.sql(
        #     """
        #     SET s3_region='us-east-1';
        #     SET s3_endpoint='https://sjc1.vultrobjects.com';
        #     SET s3_access_key_id='';
        #     SET s3_secret_access_key='';
        #     SET s3_url_style='path';
        #     SET s3_use_ssl=true;
        # """
        # )

        # con.sql(
        #     """
        #         CREATE SECRET delta_s3 (
        #         TYPE s3,
        #         KEY_ID '',
        #         SECRET '',
        #         REGION 'eu-east-1',
        #         SCOPE 's3://datasnake/deltalake_sensor_data_processed/'
        #     )
        # """
        # )

        query = f"""
            SELECT lat, lon, temp, humidity, country, state 
            FROM delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake_local']}') 
            LIMIT 10
        """

        df = con.execute(query).pl()
        print(df.head())
        con.close()

        data = df.to_dicts()

        return {"sensor_data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/current-sensor-data-daft-s3")
async def get_sensor_data_daft(auth_data: AuthRequest):
    try:
        print("inside api client get sensor data :")
        print("inside api client get sensor data :", auth_data)
        response = await is_authenticated(auth_data)

        con = duckdb.connect()
        con.sql("INSTALL delta; LOAD delta;")
        # io_config=io_config,

        # read_df = (
        #     daft.read_deltalake(
        #         table=read_paths["datasnake_sensor_data_processed_deltalake_remote"],
        #         io_config=io_config1,
        #         # ).into_partitions(6)
        #     )
        #     .filter((daft.col("country") == "US") & (daft.col("state") == "Oregon"))
        #     .select("lat", "lon", "temp", "humidity", "country", "state")
        #     .limit(5)
        # )

        df = daft.read_deltalake(
            table=read_paths["datasnake_sensor_data_processed_deltalake_local"]
        )
        catalog = SQLCatalog({"data": df})
        print("cataglog")
        print(catalog)
        results = daft.sql(
            """
            SELECT lat, lon, temp, humidity, country, state 
            FROM df
            LIMIT 10
            """,
            catalog=catalog,
        )

        print("initial dask df:")
        print(df.schema())
        # print(results)

        # result = read_df.collect().to_pydict()  # Triggers execution
        print("📦 Result:", results)

        # read_df = read_df.select(
        #     "lat", "lon", "temp", "humidity", "country", "state"
        # ).limit(5)

        # print("print daft dataframe:")
        # print(read_df)

        # query = f"""
        #     SELECT lat, lon, temp, humidity, country, state
        #     FROM delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake_remote']}')
        #     LIMIT 5
        # """

        # df = con.execute(query).pl()
        # print(df.head())
        # con.close()

        # data = read_df.collect().to_arrow()
        # print("pyarrow data")
        # print(data)
        # print("read_df.to_pydict() ===>")
        # print(read_df.to_pydict())

        # return {"sensor_data": results}

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
