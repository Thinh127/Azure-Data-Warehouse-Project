# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# COMMAND ----------

# Load silver_payments table and transform for fact_payment
silver_payments_df = spark.sql("SELECT * FROM silver_payments")
fact_payment_df = silver_payments_df.selectExpr(
    "payment_id",
    "CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT) AS date_key",  # Converting date to integer key
    "amount",
    "rider_id AS user_id"  # Renaming rider_id to user_id
)

# Load silver_riders table and transform for dim_user
silver_riders_df = spark.sql("SELECT * FROM silver_riders")
dim_user_df = silver_riders_df.select(
    col("rider_id").alias("user_id"),
    col("first"),
    col("last"),
    col("birthday").alias("bod"),
    col("is_member")
)

# Load silver_stations table and transform for dim_station
silver_stations_df = spark.sql("SELECT * FROM silver_stations")
dim_station_df = silver_stations_df.select(
    col("station_id").cast("int").alias("station_id"),  # Converting to integer if needed
    col("name"),
    col("latitude"),
    col("longitude")
)

# Load silver_trips table and transform for dim_trip and fact_trip
silver_trips_df = spark.sql("SELECT * FROM silver_trips")
dim_trip_df = silver_trips_df.select(
    col("trip_id").cast("int").alias("trip_id"),  # Convert trip_id to int if needed
    col("rideable_type").alias("ridable_type"),
    col("start_at").alias("started_at"),
    col("ended_at").alias("ended_at"),
    col("start_station_id").cast("int").alias("start_station_id"),
    col("end_station_id").cast("int").alias("end_station_id")
)

# Join silver_trips with silver_riders to access the birthday column
trips_with_riders_df = silver_trips_df.join(silver_riders_df, on="rider_id", how="left")

# Calculate user age and duration for fact_trip
fact_trip_df = trips_with_riders_df.select(
    col("rider_id").cast("int").alias("user_id"),
    col("trip_id").cast("int").alias("trip_id"),
    (F.datediff(F.current_date(), F.to_date(col("birthday"), 'yyyy-MM-dd')) / 365).alias("user_age"),  # Age calculation
    (F.unix_timestamp(col("ended_at")) - F.unix_timestamp(col("start_at"))).alias("duration")  # Duration calculation
)

# COMMAND ----------

from pyspark.sql.functions import col, datediff, current_date, to_date, unix_timestamp


def cast_to_table_schema(df, table_name):
    """Casts the columns of the input DataFrame to match the schema of the specified Delta table."""
    table_schema = spark.table(table_name).schema
    casted_df = df.select([col(field.name).cast(field.dataType) for field in table_schema])
    return casted_df

def transform_fact_payment():
    """Transforms silver_payments table to match the schema of fact_payment table."""
    silver_payments_df = spark.sql("SELECT * FROM silver_payments")
    fact_payment_df = silver_payments_df.selectExpr(
        "payment_id",
        "CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT) AS date_key",
        "amount",
        "rider_id AS user_id"
    )
    return fact_payment_df

def transform_dim_user():
    """Transforms silver_riders table to match the schema of dim_user table."""
    silver_riders_df = spark.sql("SELECT * FROM silver_riders")
    dim_user_df = silver_riders_df.select(
        col("rider_id").alias("user_id"),
        col("first"),
        col("last"),
        col("birthday").alias("bod"),
        col("is_member")
    )
    return dim_user_df

def transform_dim_station():
    """Transforms silver_stations table to match the schema of dim_station table."""
    silver_stations_df = spark.sql("SELECT * FROM silver_stations")
    dim_station_df = silver_stations_df.select(
        col("station_id").cast("int").alias("station_id"),
        col("name"),
        col("latitude"),
        col("longitude")
    )
    return dim_station_df

def transform_dim_trip():
    """Transforms silver_trips table to match the schema of dim_trip table."""
    silver_trips_df = spark.sql("SELECT * FROM silver_trips")
    dim_trip_df = silver_trips_df.select(
        col("trip_id").cast("int").alias("trip_id"),
        col("rideable_type").alias("ridable_type"),
        col("start_at").alias("started_at"),
        col("ended_at").alias("ended_at"),
        col("start_station_id").cast("int").alias("start_station_id"),
        col("end_station_id").cast("int").alias("end_station_id")
    )
    return dim_trip_df

def transform_fact_trip():
    """Joins silver_trips and silver_riders to create fact_trip with user age and trip duration."""
    silver_trips_df = spark.sql("SELECT * FROM silver_trips")
    silver_riders_df = spark.sql("SELECT * FROM silver_riders")
    
    # Join silver_trips with silver_riders to access birthday for age calculation
    trips_with_riders_df = silver_trips_df.join(silver_riders_df, on="rider_id", how="left")
    
    fact_trip_df = trips_with_riders_df.select(
        col("rider_id").cast("int").alias("user_id"),
        col("trip_id").cast("int").alias("trip_id"),
        (datediff(current_date(), to_date(col("birthday"), 'yyyy-MM-dd')) / 365).alias("user_age"),
        (unix_timestamp(col("ended_at")) - unix_timestamp(col("start_at"))).alias("duration")
    )
    return fact_trip_df

def main():
    # Dictionary mapping table names to transformed DataFrames
    dataframes = {
        "gold.fact_payments": transform_fact_payment(),
        "gold.dim_users": transform_dim_user(),
        "gold.dim_stations": transform_dim_station(),
        "gold.dim_trips": transform_dim_trip(),
        "gold.fact_trips": transform_fact_trip()
    }

    # Iterate over each DataFrame, cast it to the target schema, and write to the Delta table
    for table_name, df in dataframes.items():
        casted_df = cast_to_table_schema(df, table_name)
        casted_df.write.format("delta").mode("append").saveAsTable(table_name)

    print("All DataFrames have been successfully transformed, cast, and written to their respective tables.")

if __name__ == '__main__':
    main()


# COMMAND ----------

def cast_to_table_schema(df, table_name):
    """
    Casts the columns of the input DataFrame to match the schema of the specified Delta table.
    
    Parameters:
        df (DataFrame): The DataFrame to be casted.
        table_name (str): The name of the target table in the Delta Lake (e.g., "gold.fact_payment").
        
    Returns:
        DataFrame: The input DataFrame with columns cast to match the schema of the specified table.
    """
    # Retrieve the schema of the specified table
    table_schema = spark.table(table_name).schema

    # Cast each column to the corresponding data type in the table schema
    casted_df = df.select([col(field.name).cast(field.dataType) for field in table_schema])
    
    return casted_df


# COMMAND ----------

def main():
    # Define a mapping of DataFrames to table names
    dataframes = {
        "gold.fact_payments": fact_payment_df,
        "gold.dim_users": dim_user_df,
        "gold.dim_stations": dim_station_df,
        "gold.dim_trips": dim_trip_df,
        "gold.fact_trips": fact_trip_df
    }

    # Iterate over each DataFrame and corresponding table name
    for table_name, df in dataframes.items():
        # Cast the DataFrame to match the table schema
        casted_df = cast_to_table_schema(df, table_name)
        
        # Write the casted DataFrame to the Delta table in append mode
        casted_df.write.format("delta").mode("append").saveAsTable(table_name)
