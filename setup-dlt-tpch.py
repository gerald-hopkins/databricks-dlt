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
# MAGIC SELECT * FROM bronze.customer_dlt_raw
