-- Consolidate FHV Trip Data
CREATE OR REPLACE VIEW dev.public.fhv_tripdata AS
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2022_01
UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2022_02
UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2022_03
UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2022_04
UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2022_05

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2022_06

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2023_01

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2023_02

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2023_03

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2023_04

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2023_05

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2023_06

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2024_01

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2024_02

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2024_03

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2024_04

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2024_05

UNION ALL
SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID AS pickup_location_id,
    DOlocationID AS dropoff_location_id,
    SR_Flag,
    Affiliated_base_number
FROM
    dev.public.fhv_tripdata_2024_06;

-- Consolidate Green Trip Data
CREATE OR REPLACE VIEW dev.public.green_tripdata AS
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2022_01
UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2022_02
UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2022_03
UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2022_04
UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2022_05
UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2022_06
UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2023_01
UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2023_02

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2023_03

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2023_04

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2023_05

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2023_06

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2024_01

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2024_02

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2024_03

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2024_04

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2024_05

UNION ALL
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
FROM
    dev.public.green_tripdata_2024_06;

-- Consolidate Yellow Trip Data
CREATE OR REPLACE VIEW dev.public.yellow_tripdata AS
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2022_01

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2022_02

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2022_03

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2022_04

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2022_05

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2022_06

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2023_01
UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2023_02
UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2023_03
UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2023_04
UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2023_05
UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2023_06

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2024_01
UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2024_02
UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2024_03

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2024_04
UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2024_05

UNION ALL
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM
    dev.public.yellow_tripdata_2024_06;
