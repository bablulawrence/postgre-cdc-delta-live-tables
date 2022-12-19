# Databricks notebook source
# import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# BOOTSTRAP_SERVERS = "az-cslabs-ph2-event-hub1.servicebus.windows.net:9093"; 
EH_NS_NAME = "az-cslabs-event-hub-ns"
BOOTSTRAP_SERVERS = f"{EH_NS_NAME}.servicebus.windows.net:9093"; 
SAKEY = "9Pd1a6FG2IGYdry/bk7CLQWkihoxhZkTVDVds5EiTpk="
CONN_STRING = f'Endpoint=sb://{EH_NS_NAME}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={SAKEY}'
LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
EH_SASL = f'{LOGIN_MODULE} required username="$ConnectionString" password="{CONN_STRING}";'
TOPIC = "retail.public.sales_orders"

df = ( spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
);

#Convert from base64 to string
df = (df.withColumn("key", df["key"].cast("string"))
        .withColumn("value",df["value"].cast("string")))

# df.display()
