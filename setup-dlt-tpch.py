# Databricks notebook source


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gerald_hopkins_workspace.etl

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.orders_dlt_raw DEEP CLONE samples.tpch.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customer_dlt_raw DEEP CLONE samples.tpch.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze.customer_dlt_raw
# MAGIC ADD COLUMNS (
# MAGIC   _src_action STRING,  -- possible values I, D, T
# MAGIC   _src_insert_dt TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronze.customer_dlt_raw
# MAGIC SET 
# MAGIC   _src_action = 'I',
# MAGIC   _src_insert_dt = CURRENT_TIMESTAMP() - INTERVAL '3 DAYS'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.orders_dlt_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM bronze.orders_dlt_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.customer_dlt_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM bronze.orders_dlt_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add records to stream table so we can process INCREMENTAL data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.orders_dlt_raw
# MAGIC SELECT * FROM samples.tpch.orders
# MAGIC LIMIT 10000

# COMMAND ----------

# MAGIC %md
# MAGIC ## create a managed volume to hold source files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS etl.landing

# COMMAND ----------

# MAGIC %md
# MAGIC ## add folders for files and schema

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/gerald_hopkins_workspace/etl/landing/files')
dbutils.fs.mkdirs('/Volumes/gerald_hopkins_workspace/etl/landing/autoloader/schemas/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## add record to the stream to test SCD1 and SCD2

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.customer_dlt_raw
# MAGIC VALUES (
# MAGIC   6, 'Customer#000412450','wwodlnaoetao',20,'30-293-656-4951',4406.28,'BUILDING','New Changed Record', 'I',current_timestamp()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## add a record for backloading to Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.customer_dlt_raw
# MAGIC VALUES (
# MAGIC   6,'Customer#000412450','wwodlnaoetao',20,'30-293-656-4951',4406.28,'BUILDING','Old Record for Backloading', 'I',current_timestamp() - INTERVAL '1 DAYS'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## delete a record from Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.customer_dlt_raw
# MAGIC VALUES (
# MAGIC   6,'Customer#000412450','wwodlnaoetao',20,'30-293-656-4951',4406.28,'BUILDING','Old Record for Backloading', 'D',current_timestamp()
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## add a record to raw to truncate the SCD1 table

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.customer_dlt_raw
# MAGIC VALUES (
# MAGIC   null,'Customer#000412450','wwodlnaoetao',20,'30-293-656-4951',4406.28,'BUILDING','Old Record for Backloading', 'T',current_timestamp()
# MAGIC )
