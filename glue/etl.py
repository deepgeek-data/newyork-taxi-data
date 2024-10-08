import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date, col, when
from awsglue.dynamicframe import DynamicFrame

# Initialize the Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)

# Get the arguments passed to the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SecretName', 'TempDir', 'RedshiftSchema', 'RedshiftCluster',
                                     'RedshiftDatabase'])

# Start the Glue job
job.init(args['JOB_NAME'], args)


# Function to load data from Glue catalog
def load_tripdata(glue_context, database, table_name):
    return glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name,
        transformation_ctx=f"{table_name}_ctx"
    ).toDF()


# Initialize empty DataFrames for each type of tripdata
fhv_tripdata_df = None
green_tripdata_df = None
yellow_tripdata_df = None

# Define years and months
years = [2022, 2023, 2024]
months = ["01", "02", "03", "04", "05", "06"]

# Database name
database_name = "newyork-taxi-data"

# Loop over years and months to read and load data for each table
for year in years:
    for month in months:
        # Table names
        fhv_table = f"fhv_tripdata_{year}_{month}_parquet"
        green_table = f"green_tripdata_{year}_{month}_parquet"
        yellow_table = f"yellow_tripdata_{year}_{month}_parquet"

        # Load DataFrames from Glue catalog
        fhv_df = load_tripdata(glueContext, database_name, fhv_table)
        green_df = load_tripdata(glueContext, database_name, green_table)
        yellow_df = load_tripdata(glueContext, database_name, yellow_table)

        # Apply transformations for each DataFrame

        # Transformations for FHV Trip Data
        fhv_df = fhv_df.withColumn("pickup_datetime", to_date(col("pickup_datetime"), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn("dropOff_datetime", to_date(col("dropOff_datetime"), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumnRenamed("PUlocationID", "pickup_location_id") \
            .withColumnRenamed("DOlocationID", "dropoff_location_id") \
            .filter(col("pickup_datetime").isNotNull())  # Filter out rows with null pickup time

        # Transformations for Green Trip Data
        green_df = green_df.withColumn("lpep_pickup_datetime",
                                       to_date(col("lpep_pickup_datetime"), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn("lpep_dropoff_datetime", to_date(col("lpep_dropoff_datetime"), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumnRenamed("PULocationID", "pickup_location_id") \
            .withColumnRenamed("DOLocationID", "dropoff_location_id") \
            .withColumn("trip_type", when(col("trip_type") == 1, "Street-Hail").otherwise("Dispatch")) \
            .filter(col("lpep_pickup_datetime").isNotNull())  # Filter out rows with null pickup time

        # Transformations for Yellow Trip Data
        yellow_df = yellow_df.withColumn("tpep_pickup_datetime",
                                         to_date(col("tpep_pickup_datetime"), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn("tpep_dropoff_datetime", to_date(col("tpep_dropoff_datetime"), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumnRenamed("PULocationID", "pickup_location_id") \
            .withColumnRenamed("DOLocationID", "dropoff_location_id") \
            .withColumn("payment_type", when(col("payment_type") == 1, "Credit Card")
                        .when(col("payment_type") == 2, "Cash")
                        .when(col("payment_type") == 3, "No Charge")
                        .when(col("payment_type") == 4, "Dispute")
                        .otherwise("Unknown")) \
            .filter(col("tpep_pickup_datetime").isNotNull())  # Filter out rows with null pickup time

        # Append to the respective DataFrames
        fhv_tripdata_df = fhv_df if fhv_tripdata_df is None else fhv_tripdata_df.union(fhv_df)
        green_tripdata_df = green_df if green_tripdata_df is None else green_tripdata_df.union(green_df)
        yellow_tripdata_df = yellow_df if yellow_tripdata_df is None else yellow_tripdata_df.union(yellow_df)

# Redshift connection options
secret_name = args['SecretName']
secrets_manager = boto3.client('secretsmanager')
response = secrets_manager.get_secret_value(SecretId=secret_name)
secret = json.loads(response['SecretString'])
redshift_password = secret['password']

redshift_connection_options = {
    "url": f"jdbc:redshift://{args['RedshiftCluster']}.us-west-2.redshift.amazonaws.com:5439/{args['RedshiftDatabase']}",
    "user": "newyork-taxi-data",
    "password": redshift_password,
    "redshiftTmpDir": args['TempDir'],
    "schema": "public"  # Explicitly setting the schema to public
}

# Load the DataFrames into Redshift tables
for year in years:
    for month in months:
        # Load data for each type of trip (FHV, Green, Yellow)
        trip_data = {
            "fhv_tripdata": fhv_tripdata_df.filter(
                col("pickup_datetime").between(f"{year}-{month}-01", f"{year}-{month}-28")),
            "green_tripdata": green_tripdata_df.filter(
                col("lpep_pickup_datetime").between(f"{year}-{month}-01", f"{year}-{month}-28")),
            "yellow_tripdata": yellow_tripdata_df.filter(
                col("tpep_pickup_datetime").between(f"{year}-{month}-01", f"{year}-{month}-28"))
        }

        # Write each filtered dataframe to corresponding Redshift table
        for trip_type, df in trip_data.items():
            table_name = f"{trip_type}_{year}_{month}"  # Table name format as requested
            redshift_connection_options["table"] = table_name
            dynamic_frame = DynamicFrame.fromDF(df, glueContext, f"{trip_type}_{year}_{month}_df")

            # Write the data into Redshift
            glueContext.write_dynamic_frame.from_jdbc_conf(
                frame=dynamic_frame,
                catalog_connection="redshift",
                connection_options=redshift_connection_options,
                transformation_ctx=f"redshift_sink_{trip_type}_{year}_{month}"
            )

# Commit the Glue job
job.commit()
