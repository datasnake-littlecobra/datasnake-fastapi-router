USE datasnake;

CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_by_lat_lon AS
SELECT lat, lon, temp, humidity, country, state
FROM sensor_data_processed
WHERE lat IS NOT NULL AND lon IS NOT NULL
PRIMARY KEY ((lat, lon));
