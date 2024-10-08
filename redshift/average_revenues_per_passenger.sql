SELECT
    trip_type,
    SUM(total_amount) / SUM(passenger_count) AS avg_revenue_per_passenger
FROM (
    SELECT 'fhv_tripdata' AS trip_type, passenger_count, NULL AS total_amount FROM dev.public.public.fhv_tripdata
    UNION ALL
    SELECT 'green_tripdata' AS trip_type, passenger_count, total_amount FROM dev.public.public.green_tripdata
    UNION ALL
    SELECT 'yellow_tripdata' AS trip_type, passenger_count, total_amount FROM dev.public.public.yellow_tripdata
) AS revenue_per_passenger
WHERE passenger_count > 0
GROUP BY
    trip_type;
