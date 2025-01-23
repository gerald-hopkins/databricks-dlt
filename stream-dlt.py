# Databricks notebook source
import dlt
from pyspark.sql.functions import expr

# COMMAND ----------

# MAGIC %md
# MAGIC ## set variable to get configuration input variable for order status so we can create an gold agg table for each order status

# COMMAND ----------

_order_status = spark.conf.get('custom.orderStatus', 'NA')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Data Quality Rules

# COMMAND ----------

_order_rules = {
    "Valid Order Status":"o_orderstatus IN ('O','F','P')",
    "Valid Order Price":"o_totalprice > 0.0"
}

_customer_rules = {
    "Valid Market Segment":"c_mktsegment IS NOT NULL"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE Create a streaming table for Orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### stream orders data from Orders_dlt_raw; Data Quality Rules for Orders

# COMMAND ----------

from pyspark.sql.functions import col

@dlt.table(
    table_properties = {'quality': 'bronze'},
    comment = 'orders bronze table'
)
@dlt.expect_all_or_drop(_order_rules)  
def orders_dlt_bronze():
    df = spark.readStream.table('gerald_hopkins_workspace.bronze.orders_dlt_raw')
    df.printSchema()
    return df



# # Capture dropped records for remediation
# @dlt.table(
#     table_properties={"quality": "bronze"},
#     comment="Dropped records for remediation",
# )
# def orders_dlt_bronze_dropped():
#     df = dlt.read_stream('orders_dlt_bronze')

#     # Use the `_expectations` column to filter dropped records
#     dropped_records = df.filter(col("_expectations").isNotNull())
#     return dropped_records

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

# MAGIC %md
# MAGIC ## Apply Order Data Quality in a temporary table

# COMMAND ----------

quarantine_rules = "NOT({0})".format(" AND ".join(_order_rules.values()))

# @dlt.view
# def raw_trips_data():
#   return spark.readStream.table("samples.nyctaxi.trips")

@dlt.table(
  temporary=True,
  partition_cols=["_is_quarantined"],
)
@dlt.expect_all(_order_rules)
def order_dlt_quarantine():
  return (
    dlt.readStream("orders_dlt_union_bronze").withColumn("_is_quarantined", expr(quarantine_rules))
  )

@dlt.view
def valid_order_data():
  return dlt.readStream("order_dlt_quarantine").filter("_is_quarantined=false")

@dlt.view
def invalid_order_data():
  return dlt.readStream("order_dlt_quarantine").filter("_is_quarantined=true")

# COMMAND ----------

@dlt.table(
    table_properties = {'quality':'bronze'},
    comment = 'order quarantined data'
)
def order_dlt_quarantine_vw():
    df = dlt.readStream("invalid_order_data")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a materialized view for Customer

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
# MAGIC ## create a materialized view for Customer; Data Quality Rules for Customer

# COMMAND ----------

@dlt.view(
    comment = 'customer bronze view',
)
@dlt.expect_all(_customer_rules)
def customer_dlt_bronze_vw():
    df = spark.readStream.table('gerald_hopkins_workspace.bronze.customer_dlt_raw')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD 1 Customer

# COMMAND ----------

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
    df_o = spark.readStream.table('LIVE.valid_order_data')
                            
    df_join = df_o.join(df_c, how = 'left_outer', on = df_c.c_custkey == df_o.o_custkey)

    return df_join


# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER create and load table for joined orders and customer data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,count,sum

@dlt.view(
    #table_properties = {'quality': 'silver'},
    comment = 'joined silver table',
    name = 'orders_dlt_silver'
)
def joined_dlt_silver():
    df = spark.readStream.table('LIVE.joined_vw').withColumn('_insert_date', current_timestamp())
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD Find the order count and sum of total price by c_mktsegment

# COMMAND ----------

@dlt.view(
    #table_properties = {'quality': 'gold'},
    comment = 'aggregated gold table',
)
def orders_dlt_agg_gold():
    df = spark.readStream.table('LIVE.orders_dlt_silver')

    df_final = df.groupBy('c_mktsegment').agg(count('o_orderkey').alias('orders_count'),sum('o_totalprice').alias('sales_sum')).withColumn('_insert_date', current_timestamp())

    return df_final


# COMMAND ----------

for _status in _order_status.split(','):
    def create_table(status):
        @dlt.view(
            #table_properties={'quality': 'gold'},
            comment=f'aggregated gold table for {status}',
            name=f'orders_dlt_agg_gold_{status}'
        )
        def func():
            df = spark.readStream.table('LIVE.orders_dlt_silver')
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
