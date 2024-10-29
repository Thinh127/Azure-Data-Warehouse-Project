# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

def read_silver_table(table_name):
    return spark.read.format("delta") \
            .load(f"dbfs:/user/hive/warehouse/{table_name}")

# COMMAND ----------

silver_payments_df = read_silver_table("silver_payments")
silver_riders_df = read_silver_table("silver_riders")
silver_trips_df = read_silver_table("silver_trips")
silver_stations_df = read_silver_table("silver_stations")


# COMMAND ----------

display(silver_payments_df.limit(3))
display(silver_riders_df.limit(3))
display(silver_trips_df.limit(3))
display(silver_stations_df.limit(3))

# COMMAND ----------

# silver_payments_df leftJoin silver_riders_df leftJoin silver_trips_df => fact_payments_df

fact_payments_df = silver_payments_df.join(silver_trips_df, on='rider_id', how='left')
fact_payments_df = fact_payments_df.selectExpr(
                    "payment_id",
                    "CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT) AS date_key",
                    "amount",
                    "rider_id AS user_id"
                )
display(fact_payments_df.limit(3))

# COMMAND ----------

# fact_trip_df <= silver_trips_df leftJoin silver_riders_df
trips_with_riders_df = silver_trips_df.join(silver_riders_df, on="rider_id", how="inner")

fact_trips_df = trips_with_riders_df.select(
    F.col("rider_id").cast("int").alias("user_id"),
    F.col("trip_id").cast("string").alias("trip_id"),
    (F.datediff(F.current_date(), F.to_date(F.col("birthday"), 'yyyy-MM-dd')) / 365).alias("user_age"),
    (F.unix_timestamp(col("ended_at")) - F.unix_timestamp(col("start_at"))).alias("duration")
)

display(fact_trips_df.limit(3))

# COMMAND ----------

dim_users_df = silver_riders_df.select(
        col("rider_id").alias("user_id"),
        col("first"),
        col("last"),
        col("birthday").alias("bod"),
        col("is_member")
    )

display(dim_users_df.limit(3))

# COMMAND ----------

dim_stations_df = silver_stations_df.select(
        col("station_id").cast("string").alias("station_id"),
        col("name"),
        col("latitude"),
        col("longitude")
    )
display(dim_stations_df.limit(3))

# COMMAND ----------

dim_trips_df = silver_trips_df.select(
        col("trip_id").cast("string").alias("trip_id"),
        col("rideable_type").alias("ridable_type"),
        col("start_at").alias("started_at"),
        col("ended_at").alias("ended_at"),
        col("start_station_id").cast("string").alias("start_station_id"),
        col("end_station_id").cast("string").alias("end_station_id")
)

display(dim_trips_df.limit(3))

# COMMAND ----------

# Dictionary mapping table names to transformed DataFrames
dataframes = {
    "gold.fact_payments": fact_payments_df,
    "gold.dim_users": dim_users_df,
    "gold.dim_stations": dim_stations_df,
    "gold.dim_trips": dim_trips_df,
    "gold.fact_trips": fact_trips_df
}

fact_trips_df = fact_trips_df.withColumn("user_age", fact_trips_df["user_age"].cast(T.IntegerType()))
fact_trips_df = fact_trips_df.withColumn("duration", fact_trips_df["duration"].cast(T.FloatType()))
fact_payments_df = fact_payments_df.withColumn("amount", fact_payments_df["amount"].cast(T.DoubleType()))


# Write each DataFrame to its respective Delta table
for table_name, df in dataframes.items():
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)
