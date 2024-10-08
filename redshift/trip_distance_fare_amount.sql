SELECT
    trip_type,
    CORR(trip_distance, fare_amount) AS distance_fare_correlation
FROM (
    SELECT 'fhv_tripdata' AS trip_type, trip_distance, fare_amount FROM dev.public.public.fhv_tripdata
    UNION ALL
    SELECT 'green_tripdata' AS trip_type, trip_distance, fare_amount FROM dev.public.public.green_tripdata
    UNION ALL
    SELECT 'yellow_tripdata' AS trip_type, trip_distance, fare_amount FROM dev.public.public.yellow_tripdata
) AS distance_fare_data
GROUP BY
    trip_type;
