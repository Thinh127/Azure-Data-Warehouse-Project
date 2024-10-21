# Databricks notebook source
import os
import pyspark.sql.types as T
from pyspark.sql.types import StructField, StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ### define schema for each table

# COMMAND ----------

# Define schema for Rider table
rider_schema = StructType([
    StructField("rider_id", T.IntegerType(), True),
    StructField("first", T.StringType(), True),
    StructField("last", T.StringType(), True),
    StructField("address", T.StringType(), True),
    StructField("birthday", T.DateType(), True),
    StructField("account_start_date", T.DateType(), True),
    StructField("account_end_date", T.DateType(), True),
    StructField("is_member", T.BooleanType(), True)
])

# Define schema for Payment table
payment_schema = StructType([
    StructField("payment_id", T.IntegerType(), True),
    StructField("date", T.DateType(), True),
    StructField("amount", T.DecimalType(10, 2), True),
    StructField("rider_id", T.IntegerType(), True)
])

# Define schema for Station table
station_schema = StructType([
    StructField("station_id", T.StringType(), True),
    StructField("name", T.StringType(), True),
    StructField("latitude", T.FloatType(), True),
    StructField("longitude", T.FloatType(), True)
])

# Define schema for Trip table
trip_schema = StructType([
    StructField("trip_id", T.StringType(), True),
    StructField("rideable_type", T.StringType(), True),
    StructField("start_at", T.TimestampType(), True),
    StructField("ended_at", T.TimestampType(), True),
    StructField("start_station_id", T.StringType(), True),
    StructField("end_station_id", T.StringType(), True),
    StructField("rider_id", T.IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### utils function

# COMMAND ----------

def csv_to_bronze_table(table_name, schema, over_write=False):
    load_path = os.path.join(landing_folder, f"{table_name}.csv")
    df = spark.read. format("csv") \
        .schema(schema) \
        .option("header", "true") \
        .option("sep", ",") \
        .load(load_path)
    print(f"load successfully {table_name}.csv")

    save_path = f"/delta/bronze_{table_name}"

    if over_write:
        df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(save_path)
    else:
        df.write.format("delta") \
            .save(save_path)
    print(f"save successfully {table_name} into bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### main

# COMMAND ----------

def main():
    landing_folder = "/FileStore/landing/"
    target_tables = ('payments', 'riders', 'stations', 'trips')
    target_schemas = (payment_schema, rider_schema, station_schema, trip_schema)
    for tb, sch in zip(target_tables, target_schemas):
        csv_to_bronze_table(tb, sch, over_write=True)

# COMMAND ----------

if __name__ == '__main__':
    main()
