from fastapi import APIRouter, Query
import polars as pl
import duckdb
import os
import logging
from deltalake import DeltaTable
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("dashboard-pipeline.log"), logging.StreamHandler()],
)
import sys

# import locale

# Set UTF-8 encoding manually
sys.stdout.reconfigure(encoding="utf-8")
# locale.setlocale(locale.LC_ALL, 'C.UTF-8')

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


@router.get("/temperature-vs-hourly-data-experimental")
async def get_temperature_vs_hourly_data_duckdb():
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

    # print(
    #     f"scanning deltalake: {read_paths['datasnake_sensor_data_process_deltalake_remote']}"
    # )

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


@router.get("/temperature-vs-hourly-data-debug")
async def get_temperature_vs_hourly_data_debug():
    """
    Fetch hourly temperature data from Delta Lake.
    """
    # s3_storage_options = {
    #     "AWS_ACCESS_KEY_ID": access_key,
    #     "AWS_SECRET_ACCESS_KEY": secret_key,
    #     "AWS_ENDPOINT": f"https://{hostname}",
    # }
    print("TILL HERE 1")
    # 🔹 Load DeltaTable from S3 (Lazy Loading)
    dt = DeltaTable(
        "s3://datasnake/deltalake_sensor_data_processed",
        storage_options={
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_ENDPOINT": "https://sjc1.vultrobjects.com",
        },
    )

    # 🔹 Connect DuckDB
    con = duckdb.connect()

    # Convert to Polars DataFrame
    print("TILL HERE 2")
    pl_df = pl.from_arrow(dt.to_pyarrow_table())
    print(pl_df.schema)

    pl_df = pl_df.with_columns(
        pl.col("timestamp").str.strptime(
            pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=True
        )
    )

    print(pl_df.schema)

    con.register("raw_delta_polars_df", pl_df)

    latest_dates_df = con.execute(
        """
        SELECT DISTINCT strftime(timestamp, '%Y-%m-%d') AS date
        FROM raw_delta_polars_df
        ORDER BY date DESC
        LIMIT 50;
    """
    ).pl()

    print("Recent distinct dates from DeltaLake:")
    print(latest_dates_df)

    # distinct_hours_df = con.execute(
    #     """
    #     SELECT DISTINCT strftime(timestamp, '%H') AS hour
    #     FROM raw_delta_polars_df
    #     WHERE strftime(timestamp, '%Y-%m-%d') = (
    #         SELECT MAX(strftime(timestamp, '%Y-%m-%d')) FROM raw_delta_polars_df
    #     )
    #     ORDER BY hour ASC;
    # """
    # ).pl()

    # print("Distinct hours for the latest date:")
    # print(distinct_hours_df)

    distinct_date_data_df = con.execute(
        """
        WITH latest_date AS (
            SELECT MAX(strftime(timestamp, '%Y-%m-%d')) AS max_date
            FROM raw_delta_polars_df
        ),
        filtered_data AS (
            SELECT 
                strftime(timestamp, '%Y-%m-%d') AS date,  -- Extract Date
                strftime(timestamp, '%H') AS hour,        -- Extract Hour
                temp
            FROM raw_delta_polars_df
            WHERE strftime(timestamp, '%Y-%m-%d') = (SELECT max_date FROM latest_date)  -- Filter for Max Date
        )
        SELECT * FROM filtered_data
        ORDER BY hour ASC;
        """
    ).pl()

    print("Distinct data for the latest date:")
    print(distinct_date_data_df)

    print("reading via deltatable")
    print(str(pl_df.head()).encode("utf-8", "ignore").decode("utf-8"))
    # logging.info(pl_df.head())

    # ✅ Step 1: Filter out rows where postal_code == "00000"

    filtered_df = pl_df.filter(pl.col("postal_code") != "00000")

    # ✅ Step 2: Add `date` and `hour` columns from `timestamp`
    filtered_df = filtered_df.with_columns(
        pl.col("timestamp").cast(pl.Datetime).dt.date().alias("date"),
        pl.col("timestamp").cast(pl.Datetime).dt.truncate("1h").alias("hour"),
    )
    print("PRINT filtered df")
    print(filtered_df.head())

    # ✅ Find the most recent date in the dataset
    last_date = filtered_df["date"].max()
    print(f"Most Recent Date: {last_date}")

    # ✅ Filter the last 24 hours of that date
    # start_time = datetime.combine(
    #     last_date, datetime.min.time()
    # )  # Midnight of last date
    # df = filtered_df.filter(
    #     (pl.col("timestamp") >= start_time)
    #     & (pl.col("timestamp") < start_time + timedelta(days=1))
    # )

    # ✅ Filter data for the latest date only
    latest_day_df = filtered_df.filter(pl.col("date") == last_date)

    con.register("filterd_df", latest_day_df)

    # ✅ Step 3: Perform DuckDB SQL Aggregation
    query = """
        WITH latest_date AS (
            SELECT MAX(strftime(timestamp, '%Y-%m-%d')) AS max_date
            FROM raw_delta_polars_df
        ),
        hourly_data AS (
            SELECT 
                strftime(timestamp, '%Y-%m-%d') AS date,   -- Extract Date
                strftime(timestamp, '%H') AS hour,         -- Extract Hour
                temp,
                ROW_NUMBER() OVER (PARTITION BY strftime(timestamp, '%Y-%m-%d %H') ORDER BY timestamp) AS rn
            FROM raw_delta_polars_df
            WHERE strftime(timestamp, '%Y-%m-%d') = (SELECT max_date FROM latest_date)  -- Filter for Max Date
        )
        SELECT 
            date,
            hour,
            temp
        FROM hourly_data
        WHERE rn = 1
        ORDER BY hour ASC;
    """

    agg_df = con.execute(query).pl()  # Returns as Polars DataFrame
    print("final agg_df")
    print(agg_df.head())

    # ✅ Step 4: Convert to JSON format
    temp_hourly_json = agg_df.to_dicts()

    return {"date": str(last_date), "temp_hourly": temp_hourly_json}


@router.get("/temperature-vs-hourly-data")
async def get_temperature_vs_hourly_data(
    date: str = Query(None, description="Date in YYYY-MM-DD format")
):
    """
    Fetch hourly temperature data from Delta Lake.
    """
    # s3_storage_options = {
    #     "AWS_ACCESS_KEY_ID": access_key,
    #     "AWS_SECRET_ACCESS_KEY": secret_key,
    #     "AWS_ENDPOINT": f"https://{hostname}",
    # }
    print("TILL HERE 1")
    # 🔹 Load DeltaTable from S3 (Lazy Loading)
    dt = DeltaTable(
        "s3://datasnake/deltalake_sensor_data_processed",
        storage_options={
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_ENDPOINT": "https://sjc1.vultrobjects.com",
        },
    )

    # 🔹 Connect DuckDB
    con = duckdb.connect()

    # Convert to Polars DataFrame
    print("TILL HERE 2")
    pl_df = pl.from_arrow(dt.to_pyarrow_table())
    print(pl_df.schema)

    pl_df = pl_df.with_columns(
        pl.col("timestamp").str.strptime(
            pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=True
        )
    )

    # Convert timestamp to Datetime
    # pl_df = pl_df.with_columns(pl.col("timestamp").cast(pl.Datetime))

    print(pl_df.schema)

    con.register("raw_delta_polars_df", pl_df)

    print("reading via deltatable")
    # print(str(pl_df.head()).encode("utf-8", "ignore").decode("utf-8"))
    # logging.info(pl_df.head())

    # ✅ Step 1: Filter out rows where postal_code == "00000"
    filtered_df = pl_df.filter(pl.col("postal_code") != "00000")

    # If no date is provided, find the latest available date
    if date is None:
        max_date = pl_df.select(pl.col("timestamp").dt.date()).max().item()
    else:
        max_date = datetime.strptime(date, "%Y-%m-%d").date()
        print("max_date ===> ")
        print(max_date)

    # Filter data for the selected date
    filtered_df = pl_df.filter(pl.col("timestamp").dt.date() == max_date)

    con.register("filtered_df", filtered_df)

    # SQL Query for hourly temperature
    query = """
        WITH hourly_data AS (
            SELECT 
                strftime(timestamp, '%H') AS hour, 
                temp,
                ROW_NUMBER() OVER (PARTITION BY strftime(timestamp, '%Y-%m-%d %H') ORDER BY timestamp) AS rn
            FROM filtered_df
        )
        SELECT 
            hour, 
            temp 
        FROM hourly_data
        WHERE rn = 1
        ORDER BY hour ASC;
    """

    agg_df = con.execute(query).pl()  # Returns as Polars DataFrame
    print("final agg_df")
    print(agg_df.head())

    # ✅ Step 4: Convert to JSON format
    temp_hourly_json = agg_df.to_dicts()

    return {"date": str(max_date), "temp_hourly": temp_hourly_json}


@router.get("/temperature-vs-hourly-single-date")
async def get_temperature_vs_hourly_data(
    date: str = Query(None, description="Date in YYYY-MM-DD format")
):
    """
    Fetch hourly temperature data from Delta Lake for the given date.
    """
    print("TILL HERE 1")

    # 🔹 Load DeltaTable from S3
    dt = DeltaTable(
        "s3://datasnake/deltalake_sensor_data_processed",
        storage_options={
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_ENDPOINT": "https://sjc1.vultrobjects.com",
        },
    )

    # 🔹 Load into Polars
    print("TILL HERE 2")
    pl_df = pl.from_arrow(dt.to_pyarrow_table())

    # Ensure timestamp is parsed correctly
    pl_df = pl_df.with_columns(
        pl.col("timestamp").str.strptime(
            pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=True
        )
    )

    print(pl_df.schema)

    # ✅ Filter out invalid postal codes
    pl_df = pl_df.filter(pl.col("postal_code") != "00000")

    print(pl_df.head(50))

    # Get latest date if none provided
    if date is None:
        max_date = pl_df.select(pl.col("timestamp").dt.date()).max().item()
    else:
        max_date = datetime.strptime(date, "%Y-%m-%d").date()
        print("max_date ===>", max_date)

    # Filter rows for that specific date
    filtered_df = pl_df.filter(pl.col("timestamp").dt.date() == max_date)

    # Extract hour and date for charting
    enriched_df = filtered_df.with_columns(
        [
            pl.col("timestamp").dt.strftime("%H").alias("hour"),
            pl.col("timestamp").dt.date().alias("date"),
        ]
    )

    print(enriched_df.head())

    # Deduplicate by hour (take first record per hour)
    enriched_df = (
        enriched_df.sort("timestamp")
        .unique(subset=["hour"])
        .select(["date", "hour", "temp"])
        .sort("hour")
    )

    print(enriched_df.head())

    # ✅ Format final records
    records = enriched_df.to_dicts()

    return {"records": records, "meta": {"date": str(max_date)}}
    # # Build all 24 hours => to make up for any data missing hours in a day
    # full_hours = [f"{str(h).zfill(2)}" for h in range(24)]

    # # Convert existing data to a dict for fast lookup
    # temp_by_hour = {row["hour"]: row["temp"] for row in temp_hourly_json}

    # # Fill missing hours with None (or a default)
    # temp_hourly_filled = [
    #     {"hour": hour, "temp": temp_by_hour.get(hour)} for hour in full_hours
    # ]
    # return {"date": str(max_date), "records": temp_hourly_filled}


@router.get("/temperature-humidity-hourly-data")
async def temperature_humidity_hourly_data(
    date: str = Query(None, description="Date in YYYY-MM-DD format")
):
    """
    Fetch hourly average temperature and humidity data from Delta Lake for a given date.
    """

    # 🔹 Load DeltaTable from S3
    dt = DeltaTable(
        "s3://datasnake/deltalake_sensor_data_processed",
        storage_options={
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_ENDPOINT": "https://sjc1.vultrobjects.com",
        },
    )

    # 🔹 Load into Polars DataFrame
    pl_df = pl.from_arrow(dt.to_pyarrow_table())

    # Ensure timestamp is in correct datetime format
    pl_df = pl_df.with_columns(
        pl.col("timestamp").str.strptime(
            pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=True
        )
    )

    # ✅ Filter out invalid postal codes
    pl_df = pl_df.filter(pl.col("postal_code") != "00000")

    # Determine which date to use
    if date is None:
        max_date = pl_df.select(pl.col("timestamp").dt.date()).max().item()
    else:
        max_date = datetime.strptime(date, "%Y-%m-%d").date()

    # Filter data for the given date
    filtered_df = pl_df.filter(pl.col("timestamp").dt.date() == max_date)

    # Extract hour and aggregate average temperature and humidity
    enriched_df = (
        filtered_df.with_columns(
            [
                pl.col("timestamp").dt.strftime("%H").alias("hour"),
                pl.col("timestamp").dt.date().alias("date"),
            ]
        )
        .group_by("hour")
        .agg(
            [
                pl.col("temp").mean().alias("avg_temp"),
                pl.col("humidity").mean().alias("avg_humidity"),
            ]
        )
        .sort("hour")
    )

    # ✅ Format final records
    records = [
        {
            "hour": row["hour"],
            "temperature": round(row["avg_temp"], 2),
            "humidity": round(row["avg_humidity"], 2),
        }
        for row in enriched_df.to_dicts()
    ]

    return {"records": records, "meta": {"date": str(max_date)}}
