# Databricks notebook source
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
