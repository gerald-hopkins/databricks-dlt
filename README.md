# databricks-dlt
This repo contains my code to build a Databricks ETL pipeline with Delta Live Tables.

- Set up the new schema and source delta tables for the Delta Live Tables pipeline (setup-dlt-tpch.py)
  * Create schema etl, which is where all the tables created by the DLT pipeline will be created; these will be managed (by Databricks) tables
  * DEEP CLONE an orders table and a customers table from the Databricks Samples dataset tpch
- Create the DLT Pipeline notebook (stream-dlt.py)
  * Create a streaming table for Orders by using @dlt code to stream data from bronze.orders_dlt_raw to etl.orders_dlt_bronze
  * Create a materialized view for Customer by using @dlt code to batch import data from bronze.customer_dlt_raw to etl.customer_dlt_bronze
  * Create a temporary view for joining Orders to Customer by using @dlt code to batch import data from (etl.orders_dlt_bronze left join etl.customer_dlt_bronze) to etl.joined_vw
  * Create a materialized view to hold the joined data with a new column _insert_date by using @dlt code to batch import data from etl.joined_vw to etl.orders_dlt_silver
  * Create a materialized view for the aggregation of the combined data on c_mktsegment showing sum of total price and count of orders into etl.orders_agg_gold
- Create a DLT Pipeline in the Databricks UI that uses stream-dlt.py as its code source
- Create a validation notebook to confirm that data is being moved from table to table as expected (validate-stream-dlt.py)
