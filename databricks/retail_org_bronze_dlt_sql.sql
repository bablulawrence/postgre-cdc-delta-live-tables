-- Databricks notebook source
-- MAGIC %python
-- MAGIC # import dlt
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC 
-- MAGIC # BOOTSTRAP_SERVERS = "az-cslabs-ph2-event-hub1.servicebus.windows.net:9093";
-- MAGIC EH_NS_NAME = "az-cslabs-event-hub-ns"
-- MAGIC BOOTSTRAP_SERVERS = f"{EH_NS_NAME}.servicebus.windows.net:9093"
-- MAGIC SAKEY = "UR+tdi5brOqFxphEl2rZdwszylRHA3tkwhOqsdqA464="
-- MAGIC CONN_STRING = f"Endpoint=sb://{EH_NS_NAME}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={SAKEY}"
-- MAGIC LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
-- MAGIC EH_SASL = (
-- MAGIC     f'{LOGIN_MODULE} required username="$ConnectionString" password="{CONN_STRING}";'
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # --Create Sales orders table--#
-- MAGIC 
-- MAGIC sales_orders_df = (
-- MAGIC     spark.readStream.format("kafka")
-- MAGIC     .option("kafka.bootstrap.servers", f"{EH_NS_NAME}.servicebus.windows.net:9093")
-- MAGIC     .option("subscribe", "retail.public.sales_orders")  # Saled orders topic
-- MAGIC     .option("kafka.sasl.mechanism", "PLAIN")
-- MAGIC     .option("kafka.security.protocol", "SASL_SSL")
-- MAGIC     .option("kafka.sasl.jaas.config", EH_SASL)
-- MAGIC     .option("kafka.request.timeout.ms", "60000")
-- MAGIC     .option("kafka.session.timeout.ms", "60000")
-- MAGIC     .option("failOnDataLoss", "false")
-- MAGIC     .option("startingOffsets", "earliest")
-- MAGIC     .load()
-- MAGIC )
-- MAGIC 
-- MAGIC sales_orders_df.createOrReplaceTempView("sales_orders_stream")
-- MAGIC 
-- MAGIC # # Convert from base64 to string
-- MAGIC # sales_orders_df = sales_orders_df.withColumn(
-- MAGIC #     "key", col("key").cast("string")
-- MAGIC # ).withColumn("value", col("value").cast("string"))
-- MAGIC 
-- MAGIC # # df.display()
-- MAGIC 
-- MAGIC # # Create bronze delta live table
-- MAGIC # @dlt.table(
-- MAGIC #     table_properties={"pipelines.reset.allowed": "false"},
-- MAGIC #     spark_conf={"pipelines.trigger.interval": "1 seconds"},
-- MAGIC # )
-- MAGIC # def sales_orders_bronze():
-- MAGIC #     return sales_orders_df

-- COMMAND ----------

SELECT * FROM sales_orders_stream LIMIT 5

-- COMMAND ----------

CREATE STREAMING LIVE TABLE retail.sales_orders_bronze AS
SELECT
  CAST(key AS STRING) AS key,
  CAST(value AS STRING) AS value
from
  sales_orders_stream

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # --Create Customers table--#
-- MAGIC customers_df = (
-- MAGIC     spark.readStream.format("kafka")
-- MAGIC     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
-- MAGIC     .option("subscribe", "retail.public.customers")  # Customers topic
-- MAGIC     .option("kafka.sasl.mechanism", "PLAIN")
-- MAGIC     .option("kafka.security.protocol", "SASL_SSL")
-- MAGIC     .option("kafka.sasl.jaas.config", EH_SASL)
-- MAGIC     .option("kafka.request.timeout.ms", "60000")
-- MAGIC     .option("kafka.session.timeout.ms", "60000")
-- MAGIC     .option("failOnDataLoss", "false")
-- MAGIC     .option("startingOffsets", "earliest")
-- MAGIC     .load()
-- MAGIC )
-- MAGIC 
-- MAGIC customers_df.createOrReplaceTempView("customers_stream")
-- MAGIC 
-- MAGIC # # Convert from base64 to string
-- MAGIC # customers_df = customers_df.withColumn("key", col("key").cast("string")).withColumn(
-- MAGIC #     "value", col("value").cast("string")
-- MAGIC # )
-- MAGIC 
-- MAGIC # # df.display()
-- MAGIC 
-- MAGIC # # Create bronze delta live table
-- MAGIC # @dlt.table(
-- MAGIC #     table_properties={"pipelines.reset.allowed": "false"},
-- MAGIC #     spark_conf={"pipelines.trigger.interval": "1 seconds"},
-- MAGIC # )
-- MAGIC # def customers_bronze():
-- MAGIC #     return customers_df

-- COMMAND ----------

CREATE STREAMING LIVE TABLE retail.customers_bronze AS
SELECT
  CAST(key AS STRING) AS key,
  CAST(value AS STRING) AS value
from
  customers_stream

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # --Create Products table--#
-- MAGIC products_df = (
-- MAGIC     spark.readStream.format("kafka")
-- MAGIC     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
-- MAGIC     .option("subscribe", "retail.public.customers")  # Products topic
-- MAGIC     .option("kafka.sasl.mechanism", "PLAIN")
-- MAGIC     .option("kafka.security.protocol", "SASL_SSL")
-- MAGIC     .option("kafka.sasl.jaas.config", EH_SASL)
-- MAGIC     .option("kafka.request.timeout.ms", "60000")
-- MAGIC     .option("kafka.session.timeout.ms", "60000")
-- MAGIC     .option("failOnDataLoss", "false")
-- MAGIC     .option("startingOffsets", "earliest")
-- MAGIC     .load()
-- MAGIC )
-- MAGIC 
-- MAGIC products_df.createOrReplaceTempView("products_stream")
-- MAGIC 
-- MAGIC # # Convert from base64 to string
-- MAGIC # products_df = products_df.withColumn("key", col("key").cast("string")).withColumn(
-- MAGIC #     "value", col("value").cast("string")
-- MAGIC # )
-- MAGIC 
-- MAGIC # # df.display()
-- MAGIC 
-- MAGIC # # Create bronze delta live table
-- MAGIC # @dlt.table(
-- MAGIC #     table_properties={"pipelines.reset.allowed": "false"},
-- MAGIC #     spark_conf={"pipelines.trigger.interval": "1 seconds"},
-- MAGIC # )
-- MAGIC # def products_bronze():
-- MAGIC #     return products_df

-- COMMAND ----------

CREATE STREAMING LIVE TABLE retail.products_bronze AS
SELECT
  CAST(key AS STRING) AS key,
  CAST(value AS STRING) AS value
from
  products_stream
