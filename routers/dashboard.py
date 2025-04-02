from fastapi import APIRouter
import polars as pl
import duckdb
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("dashboard-pipeline.log"), logging.StreamHandler()],
)

router = APIRouter()


@router.get("/dashboard/data")
def get_dashboard_data():
    return {"dashboard": "This is internal dashboard data"}


access_key = os.getenv("AWS_ACCESS_KEY")
secret_key = os.getenv("AWS_SECRET_KEY")
hostname = "sjc1.vultrobjects.com"

sensor_delta_s3_bucket_raw_prod = "datasnake"
sensor_delta_s3_key_raw_prod = "deltalake_sensor_data_processed"

read_paths = {
    "datasnake_sensor_data_processed_deltalake_local": "/home/resources/deltalake-sensor-data-processed",
    "datasnake_sensor_data_process_deltalake_remote": f"s3://{sensor_delta_s3_bucket_raw_prod}/{sensor_delta_s3_key_raw_prod}/",
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


@router.get("/sensor-data-temp-timestamp")
async def get_temp_timestamp_data():
    """
    Read data from Delta Lake tables and return as JSON.
    """
    con = duckdb.connect()
    duckdb.sql(
        """
        INSTALL delta;
        LOAD delta;
        """
    )

    # {'device_id': '273200253295324-DC:DA:0C:64:79:F8', 'temp': 21.27, 'humidity': 47.57,
    # 'pressure': 1023.81, 'lat': 45.41007, 'lon': -122.73083, 'alt': 0.0, 'sats': 0, 'wind_speed': 24.4,
    # 'wind_direction': 78, 'timestamp': '2025-02-21 01:10:00.874345'}
    query = f"""
        SELECT *
        FROM delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake_local']}')
        WHERE postal_code != 000000
        limit 50
        """
    check_sensor_data_df = con.query(query).pl()
    print(check_sensor_data_df.head())

    # query = f"""SELECT count(*) FROM delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake_local']}')"""
    # check_sensor_data_df = con.query(query).pl()
    # print(check_sensor_data_df.head())

    query = f"""
    WITH ranked_data AS (
    SELECT 
        temp, 
        DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) ORDER BY timestamp) AS rn
    FROM 
        delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake_local']}')
    )
    SELECT 
        temp, 
        hour 
    FROM 
        ranked_data 
    WHERE 
        rn = 1
    ORDER BY 
        hour 
    DESC
    LIMIT 50
    """
    sensor_data_temp_timestamp_df = con.query(query).pl()
    print(sensor_data_temp_timestamp_df.head())
    con.close()

    temp_timestamp_json = sensor_data_temp_timestamp_df.to_dicts()
    check_sensor_data_json = check_sensor_data_df.to_dicts()

    return {
        # "check_sensor_data_json": check_sensor_data_json,
        "temp_timestamp": temp_timestamp_json,
    }


@router.get("/comparative-temp-humidity")
async def get_comparative_data_daily():
    con = duckdb.connect()
    duckdb.sql(
        """
        INSTALL delta;
        LOAD delta;
        """
    )

    # Query to fetch temperature and humidity data
    query = f"""
        WITH ranked_data AS (
        SELECT 
            DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) AS day,
            temp,
            humidity,
            ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) ORDER BY timestamp) AS rn
        FROM 
            delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake_local']}')
        )
        SELECT 
            day,
            temp,
            humidity
        FROM 
            ranked_data
        WHERE 
            rn = 1
        ORDER BY 
            day
        LIMIT 50
        """

    # Execute the query and fetch the results
    df = con.execute(query).pl()
    print("comparitive temp humidity data:")
    print(df.head())

    # Close the connection
    con.close()

    # return JSONResponse(content=chart_data)
    temp_humidity_json = df.to_dicts()

    return {
        "temp_humidity": temp_humidity_json,
    }


# probly need to find average of temp and pressure for the day first and then use those values
# create and save more hourly | monthly | yearly
@router.get("/comparative-temp-pressure")
async def get_comparative_data_daily():
    con = duckdb.connect()
    duckdb.sql(
        """
        INSTALL delta;
        LOAD delta;
        """
    )

    # Query to fetch temperature and humidity data
    query = f"""
        WITH ranked_data AS (
        SELECT 
            DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) AS day,
            temp,
            pressure,
            ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) ORDER BY timestamp) AS rn
        FROM 
            delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake_local']}')
        )
        SELECT 
            day,
            temp,
            pressure
        FROM 
            ranked_data
        WHERE 
            rn = 1
        ORDER BY 
            day
        LIMIT 50
        """

    # Execute the query and fetch the results
    df = con.execute(query).pl()
    print("comparitive temp pressure data:")
    print(df.head())

    # Close the connection
    con.close()

    # return JSONResponse(content=chart_data)
    temp_pressure_json = df.to_dicts()

    return {
        "temp_pressure": temp_pressure_json,
    }


@router.get("/temperature-vs-hourly-data")
async def get_temperature_vs_hourly_data():
    """
    Fetch hourly temperature data from Delta Lake.
    """
    con = duckdb.connect()
    con.sql("INSTALL delta; LOAD delta;")
    con.sql("INSTALL httpfs; LOAD httpfs;")

    duckdb_storage_options = {
        "s3_access_key_id": access_key,
        "s3_secret_access_key": secret_key,
        "s3_endpoint": f"https://{hostname}",
        "s3_region": "us-east-1",
        "s3_url_style": "path",
    }

    # Dynamically set DuckDB's S3 parameters
    for key, value in duckdb_storage_options.items():
        con.sql(f"SET {key} = '{value}';")

    # con.sql(
    #     """
    #     SET s3_endpoint = 'https://sjc1.vultrobjects.com';
    #     SET s3_access_key_id = '';
    #     SET s3_secret_access_key = '';
    #     SET s3_region = 'us-east-1';
    #     SET s3_url_style: 'path';
    #     SET s3_allow_unsafe_rename: 'true';
    #     """
    # )

    print(
        f"scanning deltalake: {read_paths['datasnake_sensor_data_process_deltalake_remote']}"
    )

    query = f"""
    WITH hourly_data AS (
        SELECT 
            temp, 
            DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
            ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) ORDER BY timestamp) AS rn
        FROM delta_scan('{read_paths['datasnake_sensor_data_process_deltalake_remote']}')
    )
    SELECT 
        temp, 
        hour 
    FROM 
        hourly_data 
    WHERE 
        rn = 1
    ORDER BY 
        hour DESC
    LIMIT 100
    """

    df = con.execute(query).pl()
    print(df.head())
    con.close()

    temp_hourly_json = df.to_dicts()

    return {
        "temp_hourly": temp_hourly_json,
    }
