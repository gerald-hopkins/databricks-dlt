# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## set variable to get configuration input variable for order status so we can create an gold agg table for each order status

# COMMAND ----------

_order_status = spark.conf.get('custom.orderStatus', 'NA')

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE Create a streaming table for Orders

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

# MAGIC %md
# MAGIC ## create a streaming table for reading order files from S3 volume

# COMMAND ----------

@dlt.table(
  table_properties = {'quality': 'bronze'},
  comment = 'orders autoloader table',
  name = 'orders_autoloader_dlt_bronze'
)
def func():
  df = (
    spark
    .readStream
    .format("cloudFiles")
    .option('cloudFiles.schemaHints', 'o_orderkey long,o_custkey long,o_orderstatus string,o_totalprice decimal(18,2),o_orderdate date,o_orderpriority string,o_clerk string,o_shippriority integer,o_comment string')
    .option("cloudFiles.format", "csv")
    .option('pathGlobFilter', '*.csv')
    .option('cloudFiles.schemaLocation', '/Volumes/gerald_hopkins_workspace/etl/landing/autoloader/schemas/1/')
    .option('cloudFiles.schemaEvolutionMode','none')
    .load('/Volumes/gerald_hopkins_workspace/etl/landing/files/')
    )
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## create a streaming union table for the two streaming orders tables

# COMMAND ----------

dlt.create_streaming_table('orders_dlt_union_bronze')

@dlt.append_flow(
    target = 'orders_dlt_union_bronze'
)
def order_delta_append():
    df = spark.readStream.table('LIVE.orders_dlt_bronze')
    return df

@dlt.append_flow(
    target = 'orders_dlt_union_bronze'
)
def order_autoloader_append():
    df = spark.readStream.table('LIVE.orders_autoloader_dlt_bronze')
    return df

# COMMAND ----------

## Create a materialized view for Customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### for a materialized view the source will be batch

# COMMAND ----------

# @dlt.table(
#     table_properties = {'quality': 'bronze'},
#     comment = 'customer bronze table',
#     name = 'customer_dlt_bronze'
# )
# def cust_dlt_bronze():
#     df = spark.read.table('gerald_hopkins_workspace.bronze.customer_dlt_raw')
#     return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## create a materialized view for Customer

# COMMAND ----------

@dlt.view(
    comment = 'customer bronze view',
)
def customer_dlt_bronze_vw():
    df = spark.readStream.table('gerald_hopkins_workspace.bronze.customer_dlt_raw')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD 1 Customer

# COMMAND ----------

from pyspark.sql.functions import expr

dlt.create_streaming_table('customer_dlt_scd1_bronze')

dlt.apply_changes(
    source = 'customer_dlt_bronze_vw',
    target = 'customer_dlt_scd1_bronze',
    keys = ['c_custkey'],
    stored_as_scd_type = 1,
    apply_as_deletes = expr("_src_action = 'D'"),
    apply_as_truncates = expr("_src_action = 'T'"),
    sequence_by = '_src_insert_dt'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD 2 Customer

# COMMAND ----------

dlt.create_streaming_table('customer_dlt_scd2_bronze')

dlt.apply_changes(
    source = 'customer_dlt_bronze_vw',
    target = 'customer_dlt_scd2_bronze',
    keys = ['c_custkey'],
    stored_as_scd_type = 2,
    except_column_list = ['_src_action','_src_insert_dt'],
    sequence_by = '_src_insert_dt'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a temporary view to join customer and orders data

# COMMAND ----------

@dlt.view(
    comment = 'join customer and orders unioned bronze tables'
)
def joined_vw():
    df_c = spark.read.table('LIVE.customer_dlt_scd2_bronze').where('__END_AT = NULL')
    df_o = spark.read.table('LIVE.orders_dlt_union_bronze')
                            
    df_join = df_o.join(df_c, how = 'left_outer', on = df_c.c_custkey == df_o.o_custkey)

    return df_join


# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER create and load table for joined orders and customer data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,count,sum

@dlt.table(
    table_properties = {'quality': 'silver'},
    comment = 'joined silver table',
    name = 'orders_dlt_silver'
)
def joined_dlt_silver():
    df = spark.read.table('LIVE.joined_vw').withColumn('_insert_date', current_timestamp())
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD Find the order count and sum of total price by c_mktsegment

# COMMAND ----------

@dlt.table(
    table_properties = {'quality': 'gold'},
    comment = 'aggregated gold table',
)
def orders_dlt_agg_gold():
    df = spark.read.table('LIVE.orders_dlt_silver')

    df_final = df.groupBy('c_mktsegment').agg(count('o_orderkey').alias('orders_count'),sum('o_totalprice').alias('sales_sum')).withColumn('_insert_date', current_timestamp())

    return df_final


# COMMAND ----------

for _status in _order_status.split(','):
    def create_table(status):
        @dlt.table(
            table_properties={'quality': 'gold'},
            comment=f'aggregated gold table for {status}',
            name=f'orders_dlt_agg_gold_{status}'
        )
        def func():
            df = spark.read.table('LIVE.orders_dlt_silver')
            df_final = (df.where(f'o_orderstatus = "{status}"')
                          .groupBy('c_mktsegment')
                          .agg(
                              count('o_orderkey').alias('orders_count'),
                              sum('o_totalprice').alias('sales_sum')
                          )
                          .withColumn('_insert_date', current_timestamp()))
            return df_final
        return func

    # Call the create_table function to register the table
    create_table(_status)
