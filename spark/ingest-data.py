from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import psycopg2

# Function to execute SQL commands in PostgreSQL
def execute_postgres_sql(sql_command):
    conn = psycopg2.connect(
        host="db",
        database="postgres",
        user="postgres",
        password="secret"
    )
    cur = conn.cursor()
    cur.execute(sql_command)
    conn.commit()
    cur.close()
    conn.close()




print("========== SPARK ==========")

spark = SparkSession.builder \
    .appName("FlightDataIngestion") \
    .master("spark://spark:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "secretpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

path = "s3a://bucket/flight_data_2024.csv"
# File Contains 7,079,081 rows.

try:
    print(f"--- Attempting to read from {path} ---")
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(path)
    # Trigger an action to verify the file is accessible
    df.limit(1).collect()
except Exception as e:
    print(f"Error during reading data: {e}")
    spark.stop()
    raise

try:
    print("--- Starting Data Transformation ---")
    
    # Delete rows for canceled and diverted flights:
    df = df.filter((F.col("cancelled") == 0) & (F.col("diverted") == 0))

    # Delete Columns:
    cols_to_drop = [
        "year", "month", "day_of_month", "day_of_week", 
        "crs_dep_time", "dep_time", "wheels_on", "wheels_off", 
        "crs_arr_time", "arr_time", "cancelled", "diverted", 
        "cancellation_code"
    ]
    df = df.drop(*cols_to_drop)

    # Database Normalization: Move origin, origin_city, origin_state to a separate table (Star Schema Approach):
    origins = df.select(
        F.col("origin").alias("id"), 
        F.col("origin_city_name").alias("city"), 
        F.col("origin_state_nm").alias("state")
    )
    dests = df.select(
        F.col("dest").alias("id"), 
        F.col("dest_city_name").alias("city"), 
        F.col("dest_state_nm").alias("state")
    )

    airports_df = origins.union(dests).distinct()

    # Remove state abbreviations from City (eliminate redundancy):
    airports_df = airports_df.withColumn(
        "city", 
        F.split(F.col("city"), ",").getItem(0)
    )
    
    df = df.drop("origin_city_name", "origin_state_nm", "dest_city_name", "dest_state_nm")

    # Convert fl_date to date format:
    df = df.withColumn("fl_date", F.to_date(F.col("fl_date"), "yyyy-MM-dd"))

    # Convert op_carrier_fl_num to an int then to a string (Flight code i.e.: 4386.0 => 4386)
    df = df.withColumn("op_carrier_fl_num", F.col("op_carrier_fl_num").cast("float").cast("int").cast("string"))

    # Convert all numbers to the correct format (int):
    # distance column can be a float, but is converted to int to optimise space usage.
    int_cols = [
    "dep_delay", "taxi_out", "taxi_in", "arr_delay", "crs_elapsed_time", 
    "actual_elapsed_time", "air_time", "distance", "carrier_delay", 
    "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"
    ]

    for col_name in int_cols:
        df = df.withColumn(col_name, F.col(col_name).cast("double").cast("int"))

    # Rename columns to match the database schema:
    df = df.withColumnRenamed("origin", "origin_airport_id") \
              .withColumnRenamed("dest", "dest_airport_id")
    
    # Only one row has a missing value in the op_carrier_fl_num column, we will drop it:
    df = df.na.drop()

    # Delete any duplicates:
    df = df.dropDuplicates()

    # Define validation rules
    # taxi out, taxi in, crs_elapsed_time, actual_elapsed_time, air_time, distance, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay should be greater than or equal to 0.

    validation = (F.col("taxi_out") >= 0) & \
                 (F.col("taxi_in") >= 0) & \
                 (F.col("crs_elapsed_time") >= 0) & \
                 (F.col("actual_elapsed_time") >= 0) & \
                 (F.col("air_time") >= 0) & \
                 (F.col("distance") >= 0) & \
                 (F.col("carrier_delay") >= 0) & \
                 (F.col("weather_delay") >= 0) & \
                 (F.col("nas_delay") >= 0) & \
                 (F.col("security_delay") >= 0) & \
                 (F.col("late_aircraft_delay") >= 0)

    # 2. Add a flag or filter into two separate DataFrames
    df_with_validation = df.withColumn("is_valid", validation)

    valid_df = df_with_validation.filter("is_valid == true").drop("is_valid")
    error_df = df_with_validation.filter("is_valid == false")

    # error_df can be written to a separate table for further analysis of data quality issues, while valid_df is the cleaned dataset ready for storage or analysis. For now, we will only write the valid_df to the database.

    print("Data Transformation Completed!")
except Exception as e:
    print(f"Error during transforming data: {e}")

try:
    # Connect to the database and create the schema if it does not exist: 
    execute_postgres_sql("CREATE SCHEMA IF NOT EXISTS sch_cleaned_data")
   
    print(f"--- Creating tables in the database if they do not exist ---")
    # Create the airports table:
    execute_postgres_sql("""
    CREATE TABLE IF NOT EXISTS sch_cleaned_data.airports_table (
        id TEXT PRIMARY KEY,
        city TEXT,
        state TEXT
    )
    """)

    # Create the main flights data table (silver_table):
    execute_postgres_sql("""
    CREATE TABLE IF NOT EXISTS sch_cleaned_data.silver_table (
        id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        fl_date DATE,
        op_unique_carrier TEXT,
        op_carrier_fl_num TEXT,
        origin_airport_id TEXT REFERENCES sch_cleaned_data.airports_table(id),
        dest_airport_id TEXT REFERENCES sch_cleaned_data.airports_table(id),
        dep_delay INT,
        taxi_out INT,
        taxi_in INT,
        arr_delay INT,
        crs_elapsed_time INT,
        actual_elapsed_time INT,
        air_time INT,
        distance INT,
        carrier_delay INT,
        weather_delay INT,
        nas_delay INT,
        security_delay INT,
        late_aircraft_delay INT
    )
    """)

    # Add validation constraints to the main flights data table (silver_table):
    execute_postgres_sql("ALTER TABLE sch_cleaned_data.silver_table DROP CONSTRAINT IF EXISTS chk_non_negative_values;")
    execute_postgres_sql("""
    ALTER TABLE sch_cleaned_data.silver_table
    ADD CONSTRAINT chk_non_negative_values CHECK (
        taxi_out >= 0 AND
        taxi_in >= 0 AND
        crs_elapsed_time >= 0 AND
        actual_elapsed_time >= 0 AND
        air_time >= 0 AND
        distance >= 0 AND
        carrier_delay >= 0 AND
        weather_delay >= 0 AND
        nas_delay >= 0 AND
        security_delay >= 0 AND
        late_aircraft_delay >= 0
    )
    """)

except Exception as e:
    print(f"Error during creating tables: {e}")
    
try:
    db_url = "jdbc:postgresql://db:5432/postgres"
    properties = {
        "user": "postgres",
        "password": "secret",
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified" 
    }

    print("--- Writing Data to Database ---")
    # Truncate tables before writing to avoid duplicates when re-running the pipeline:
    execute_postgres_sql("TRUNCATE TABLE sch_cleaned_data.silver_table CASCADE;")
    execute_postgres_sql("TRUNCATE TABLE sch_cleaned_data.airports_table CASCADE;")

    # Writing airports table first to avoid foreign key constraints issues when writing the main table (silver_table):
    airports_df.write \
    .mode("append") \
    .option("batchsize", "10000") \
    .jdbc(url=db_url, table="sch_cleaned_data.airports_table", properties=properties)

    # Writing the main flights data table (silver_table):
    valid_df.write \
    .mode("append") \
    .option("batchsize", "10000") \
    .jdbc(url=db_url, table="sch_cleaned_data.silver_table", properties=properties)

except Exception as e:
    print(f"Error during writing data to database: {e}")

finally:
    spark.stop()