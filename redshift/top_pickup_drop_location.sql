SELECT
    trip_type,
    AVG(trip_duration_minutes) AS avg_duration,
    AVG(total_amount) AS avg_revenue,
    CORR(trip_duration_minutes, total_amount) AS correlation_duration_revenue
FROM (
    -- FHV Trip Data
    SELECT
        'fhv_tripdata' AS trip_type,
        EXTRACT(EPOCH FROM (dropOff_datetime - pickup_datetime))/60 AS trip_duration_minutes,
        NULL AS total_amount
    FROM
        dev.public.public.fhv_tripdata
    WHERE
        pickup_datetime IS NOT NULL AND dropOff_datetime IS NOT NULL

    UNION ALL

    -- Green Trip Data
    SELECT
        'green_tripdata' AS trip_type,
        EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime))/60 AS trip_duration_minutes,
        total_amount
    FROM
        dev.public.public.green_tripdata
    WHERE
        lpep_pickup_datetime IS NOT NULL AND lpep_dropoff_datetime IS NOT NULL

    UNION ALL

    -- Yellow Trip Data
    SELECT
        'yellow_tripdata' AS trip_type,
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60 AS trip_duration_minutes,
        total_amount
    FROM
        dev.public.public.yellow_tripdata
    WHERE
        tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL
) AS combined_data
GROUP BY
    trip_type;
