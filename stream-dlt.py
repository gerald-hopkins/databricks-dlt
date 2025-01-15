# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a streaming table for Orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### stream orders data from Orders_dlt_raw

# COMMAND ----------

@dlt.table(
    table_properties = {'quality': 'bronze'},
    comment = 'orders bronze table'
)
def orders_dlt_bronze():
    df = spark.readStream.table('gerald_hopkins_workspace.bronze.orders_dlt_raw')
    return df

# COMMAND ----------

## Create a materialized view for Customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### for a materialized view the source will be batch

# COMMAND ----------

@dlt.table(
    table_properties = {'quality': 'bronze'},
    comment = 'customer bronze table',
    name = 'customer_dlt_bronze'
)
def cust_dlt_bronze():
    df = spark.read.table('gerald_hopkins_workspace.bronze.customer_dlt_raw')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a temporary view to join customer and orders data

# COMMAND ----------

@dlt.view(
    comment = 'join customer and address bronze tables'
)
def joined_vw():
    df_c = spark.read.table('LIVE.customer_dlt_bronze')
    df_o = spark.read.table('LIVE.orders_dlt_bronze')
                            
    df_join = df_o.join(df_c, how = 'left_outer', on = df_c.c_custkey == df_o.o_custkey)

    return df_join


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,count

@dlt.table(
    table_properties = {'quality': 'silver'},
    comment = 'joined silver table',
    name = 'joined_dlt_silver'
)
def joined_dlt_silver():
    df = spark.read.table('LIVE.joined_vw').withColumn('_insert_date', current_timestamp())
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the order count by c_mktsegment

# COMMAND ----------

@dlt.table(
    table_properties = {'quality': 'gold'},
    comment = 'aggregated gold table',
)
def orders_agg_gold():
    df = spark.read.table('LIVE.joined_dlt_silver')

    df_final = df.groupBy('c_mktsegment').agg(count('o_orderkey').alias('orders_count')).withColumn('_insert_date', current_timestamp())

    return df_final

