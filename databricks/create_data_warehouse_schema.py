# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_date (
# MAGIC   date_key INT,
# MAGIC   date DATE,
# MAGIC   year INT,
# MAGIC   quarter INT,
# MAGIC   quarter_name STRING,
# MAGIC   month INT,
# MAGIC   month_name STRING,
# MAGIC   month_short_name STRING,
# MAGIC   week_of_year INT,
# MAGIC   day INT,
# MAGIC   day_name STRING,
# MAGIC   day_short_name STRING,
# MAGIC   day_of_week INT,
# MAGIC   day_of_year INT,
# MAGIC   is_weekend BOOLEAN,
# MAGIC   is_holiday BOOLEAN,
# MAGIC   holiday_name STRING,
# MAGIC   is_business_day BOOLEAN,
# MAGIC   fiscal_year INT,
# MAGIC   fiscal_quarter INT,
# MAGIC   fiscal_month INT,
# MAGIC   is_current_day BOOLEAN,
# MAGIC   is_current_month BOOLEAN,
# MAGIC   is_current_year BOOLEAN
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_user (
# MAGIC   user_id INT,
# MAGIC   first STRING,
# MAGIC   last STRING,
# MAGIC   bod DATE,
# MAGIC   is_member BOOLEAN
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_trip (
# MAGIC   trip_id INT,
# MAGIC   ridable_type STRING,
# MAGIC   started_at TIMESTAMP,
# MAGIC   ended_at TIMESTAMP,
# MAGIC   start_station_id INT,
# MAGIC   end_station_id INT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_station (
# MAGIC   station_id INT,
# MAGIC   name STRING,
# MAGIC   latitude FLOAT,
# MAGIC   longitude FLOAT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_payment (
# MAGIC   payment_id INT,
# MAGIC   user_id INT,
# MAGIC   trip_id INT,
# MAGIC   date_key INT,
# MAGIC   amount FLOAT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_trip (
# MAGIC   user_id INT,
# MAGIC   trip_id INT,
# MAGIC   user_age INT,
# MAGIC   duration FLOAT
# MAGIC ) USING DELTA;
