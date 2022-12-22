# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# BOOTSTRAP_SERVERS = "az-cslabs-ph2-event-hub1.servicebus.windows.net:9093";
EH_NS_NAME = "az-cslabs-event-hub-ns"
BOOTSTRAP_SERVERS = f"{EH_NS_NAME}.servicebus.windows.net:9093"
SAKEY = "UR+tdi5brOqFxphEl2rZdwszylRHA3tkwhOqsdqA464="
CONN_STRING = f"Endpoint=sb://{EH_NS_NAME}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={SAKEY}"
LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
EH_SASL = (
    f'{LOGIN_MODULE} required username="$ConnectionString" password="{CONN_STRING}";'
)

# COMMAND ----------

# --Create Sales orders table--#

sales_orders_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers",  f"{EH_NS_NAME}.servicebus.windows.net:9093")
    .option("subscribe", "retail.public.sales_orders")  # Saled orders topic
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
)

# Convert from base64 to string
sales_orders_df = sales_orders_df.withColumn(
    "key", col("key").cast("string")
).withColumn("value", col("value").cast("string"))

# df.display()

# Create bronze delta live table
@dlt.table(
    table_properties={"pipelines.reset.allowed": "false"},
    spark_conf={"pipelines.trigger.interval": "1 seconds"},
)
def sales_orders_bronze():
    return sales_orders_df

# COMMAND ----------

# --Create Customers table--#
customers_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers",  f"{EH_NS_NAME}.servicebus.windows.net:9093")
    .option("subscribe", "retail.public.customers")  # Customers topic
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
)

# Convert from base64 to string
customers_df = customers_df.withColumn("key", col("key").cast("string")).withColumn(
    "value", col("value").cast("string")
)

# df.display()

# Create bronze delta live table
@dlt.table(
#     table_properties={"pipelines.reset.allowed": "false"},
    spark_conf={"pipelines.trigger.interval": "1 seconds"},
)
def customers_bronze():
    return customers_df

# COMMAND ----------

# --Create Products table--#
products_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers",  f"{EH_NS_NAME}.servicebus.windows.net:9093")
    .option("subscribe", "retail.public.products")  # Products topic
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
)

# Convert from base64 to string
products_df = products_df.withColumn("key", col("key").cast("string")).withColumn(
    "value", col("value").cast("string")
)

# df.display()

# Create bronze delta live table
@dlt.table(
    table_properties={"pipelines.reset.allowed": "false"},
    spark_conf={"pipelines.trigger.interval": "1 seconds"},
)
def products_bronze():
    return products_df
