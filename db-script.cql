USE datasnake;

CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_by_lat_lon AS
SELECT 
    lat, lon, temp, humidity, country, state, postal_code, timestamp, id
FROM sensor_data_processed
WHERE 
    lat IS NOT NULL AND 
    lon IS NOT NULL AND 
    country IS NOT NULL AND 
    state IS NOT NULL AND 
    postal_code IS NOT NULL AND 
    timestamp IS NOT NULL AND 
    id IS NOT NULL
PRIMARY KEY ((lat), lon, country, state, postal_code, timestamp, id)
WITH CLUSTERING ORDER BY (lon ASC, country ASC, state ASC, postal_code ASC, timestamp DESC, id ASC);