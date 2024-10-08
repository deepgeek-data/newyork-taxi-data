SELECT
    trip_type,
    AVG(tip_amount) AS avg_tip_amount,
    AVG(total_amount) AS avg_total_amount,
    (AVG(tip_amount)/AVG(total_amount)) * 100 AS avg_tip_percentage
FROM (
    SELECT
        'green_tripdata' AS trip_type,
        tip_amount,
        total_amount
    FROM
        dev.public.public.green_tripdata

    UNION ALL

    SELECT
        'yellow_tripdata' AS trip_type,
        tip_amount,
        total_amount
    FROM
        dev.public.public.yellow_tripdata
) AS revenue_tips
GROUP BY
    trip_type;
