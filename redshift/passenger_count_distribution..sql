SELECT
    trip_type,
    passenger_count,
    COUNT(*) AS trip_count
FROM (
    SELECT 'fhv_tripdata' AS trip_type, passenger_count FROM dev.public.public.fhv_tripdata
    UNION ALL
    SELECT 'green_tripdata' AS trip_type, passenger_count FROM dev.public.public.green_tripdata
    UNION ALL
    SELECT 'yellow_tripdata' AS trip_type, passenger_count FROM dev.public.public.yellow_tripdata
) AS passenger_data
GROUP BY
    trip_type, passenger_count
ORDER BY
    trip_type, trip_count DESC;
