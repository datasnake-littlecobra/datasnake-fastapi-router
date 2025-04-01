from fastapi import APIRouter
import polars as pl
import duckdb

router = APIRouter()


@router.get("/dashboard/data")
def get_dashboard_data():
    return {"dashboard": "This is internal dashboard data"}


read_paths = {
    "datasnake_sensor_data_processed_deltalake": "/home/resources/deltalake-sensor-data-processed",
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
        FROM delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake']}')
        WHERE postal_code != 000000
        limit 50
        """
    check_sensor_data_df = con.query(query).pl()
    print(check_sensor_data_df.head())

    # query = f"""SELECT count(*) FROM delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake']}')"""
    # check_sensor_data_df = con.query(query).pl()
    # print(check_sensor_data_df.head())

    query = f"""
    WITH ranked_data AS (
    SELECT 
        temp, 
        DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) ORDER BY timestamp) AS rn
    FROM 
        delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake']}')
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
            delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake']}')
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
            delta_scan('{read_paths['datasnake_sensor_data_processed_deltalake']}')
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
