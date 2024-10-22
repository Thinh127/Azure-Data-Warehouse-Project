# Databricks notebook source
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from functools import reduce

target_tables = ['bronze_payments', 'bronze_trips', 'bronze_stations', 'bronze_riders']

# COMMAND ----------

def df_from_delta_tb(table_name):
    return spark.read.format("delta") \
            .option("header", "true") \
            .load(f"/delta/{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### processing data

# COMMAND ----------

# DBTITLE 1,payments table
df_payment = df_from_delta_tb("bronze_payments")

# COMMAND ----------

def get_null_record(df):
    # Create a condition to check nulls in all columns
    null_condition = [F.col(c).isNull() for c in df.columns]

    # Filter rows where any column contains null
    df_nulls = df.filter(reduce(lambda x, y: x | y, null_condition))

    # Show rows containing null values
    return df_nulls

def drop_null_rows(df: DataFrame, columns=None) -> DataFrame:
    """
    Drops rows with null values from the DataFrame.
    
    Parameters:
    df (DataFrame): The input DataFrame.
    columns (list): List of columns to check for null values. If None, all columns will be checked.
    
    Returns:
    DataFrame: A DataFrame with rows containing null values dropped.
    """
    if columns:
        # Drop rows where any of the specified columns contain null
        return df.dropna(subset=columns)
    return df.dropna()

def drop_duplicate_rows(df: DataFrame, columns=None) -> DataFrame:
    """
    Drops duplicate rows based on all columns from the DataFrame.
    
    Parameters:
    df (DataFrame): The input DataFrame.
    
    Returns:
    DataFrame: A DataFrame with duplicate rows removed.
    """
    if columns:
        return df.dropDuplicates(columns)
    return df.dropDuplicates()

# COMMAND ----------

df_trip = df_from_delta_tb("bronze_trips")

# COMMAND ----------

display(get_null_record(df_from_delta_tb("bronze_riders")))

# COMMAND ----------

df_riders = df_from_delta_tb('bronze_riders')

# Step 1: Select 10 records from the original DataFrame to modify
df_sample = df_riders.limit(10)

# Step 2: Modify the sample by adding nulls to 'address' for the first 3 rows and 'birthday' for the first 7 rows
df_sample = df_sample.withColumn(
    "address", F.when(F.monotonically_increasing_id() < 3, F.lit(None)).otherwise(df_sample["address"])
)

df_sample = df_sample.withColumn(
    "birthday", F.when(F.monotonically_increasing_id() < 7, F.lit(None)).otherwise(df_sample["birthday"])
)

# Step 3: Exclude the selected 10 records from the original DataFrame
df_remaining = df_riders.subtract(df_sample)

# Step 4: Union the modified sample with the remaining records
df_updated = df_remaining.union(df_sample)

# Show the updated DataFrame
df_updated.show()

# COMMAND ----------

cols = df_riders.columns
cols.remove('account_end_date')
cols

# COMMAND ----------

def main():
    # initial bronze dataframes
    bronze_dict = {key: df_from_delta_tb(key) for key in target_tables}
    for key in bronze_dict.keys():
        # drop null first
        if key != 'bronze_riders':
            bronze_dict[key] = drop_null_rows(bronze_dict[key])
        else:
            columns = bronze_dict[key].columns
            columns.remove('account_end_date')
            bronze_dict[key] = drop_null_rows(bronze_dict[key], columns=columns)
        # drop duplicates
        bronze_dict[key] = drop_duplicate_rows(bronze_dict[key])
    
