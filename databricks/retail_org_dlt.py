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
    .option("kafka.bootstrap.servers", f"{EH_NS_NAME}.servicebus.windows.net:9093")
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

# Create raw delta live table
@dlt.table(
    table_properties={"pipelines.reset.allowed": "false"},
    spark_conf={"pipelines.trigger.interval": "1 seconds"},
)
def sales_orders_raw():
    return sales_orders_df

# COMMAND ----------

# --Create Customers table--#
customers_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{EH_NS_NAME}.servicebus.windows.net:9093")
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

# Create raw delta live table
@dlt.table(
    #     table_properties={"pipelines.reset.allowed": "false"},
    spark_conf={"pipelines.trigger.interval": "1 seconds"},
)
def customers_raw():
    return customers_df

# COMMAND ----------

# --Create Products table--#
products_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{EH_NS_NAME}.servicebus.windows.net:9093")
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

# Create raw delta live table
@dlt.table(
    table_properties={"pipelines.reset.allowed": "false"},
    spark_conf={"pipelines.trigger.interval": "1 seconds"},
)
def products_raw():
    return products_df

# COMMAND ----------

sales_orders_schema = StructType(
    [
        StructField("customer_id", LongType(), True),
        StructField("customer_name", StringType(), True),
        StructField("order_datetime", StringType(), True),
        StructField("order_number", LongType(), True),
        StructField(
            "ordered_products",
            ArrayType(
                StructType(
                    [
                        StructField("curr", StringType(), True),
                        StructField("id", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("price", IntegerType(), True),
                        StructField("qty", IntegerType(), True),
                        StructField("unit", StringType(), True),
                        StructField(
                            "promotion_info",
                            StructType(
                                [
                                    StructField("promo_id", IntegerType(), True),
                                    StructField("promo_qty", IntegerType(), True),
                                    StructField("promo_disc", DecimalType(3, 2), True),
                                    StructField("promo_item", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("number_of_line_items", LongType(), True),
        StructField(
            "clicked_items", ArrayType(ArrayType(StringType(), True), True), True
        ),
        StructField(
            "promo_info",
            ArrayType(
                StructType(
                    [
                        StructField("promo_id", IntegerType(), True),
                        StructField("promo_qty", IntegerType(), True),
                        StructField("promo_disc", DecimalType(3, 2), True),
                        StructField("promo_item", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
    ]
)


@dlt.table(
    comment="Load data to sales_orders cleansed table",
    table_properties={"pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def sales_orders_cleansed():
    return (
        dlt.read_stream("sales_orders_raw")
        #         spark.readStream.format("delta").table("retail_org.sales_orders_raw")
        .select(get_json_object(col("value"), "$.payload.after").alias("row"))
        .withColumn("row", regexp_replace("row", '"\\[', "["))
        .withColumn("row", regexp_replace("row", '\\]"', "]"))
        .withColumn("row", regexp_replace("row", "\\\\", ""))
        .select(from_json(col("row"), sales_orders_schema).alias("row"))
        .select("row.*")
        .withColumn("ordered_products", explode("ordered_products"))
        .withColumn("order_datetime", from_unixtime("order_datetime"))
        .withColumn("product_id", col("ordered_products").id)
        .withColumn("unit_price", col("ordered_products").price)
        .withColumn("quantity", col("ordered_products").qty)
    )


# sales_orders_cleansed().display()

# COMMAND ----------

customers_schema = StructType(
    [
        StructField("customer_id", LongType(), False),
        StructField("tax_id", StringType(), True),
        StructField("tax_code", StringType(), True),
        StructField("customer_name", StringType(), False),
        StructField("state", StringType(), False),
        StructField("city", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("street", StringType(), False),
        StructField("number", StringType(), False),
        StructField("unit", StringType(), False),
        StructField("region", StringType(), False),
        StructField("district", StringType(), False),
        StructField("lon", DecimalType(10, 6), False),
        StructField("lat", DecimalType(10, 6), False),
        StructField("ship_to_address", StringType(), False),
        StructField(
            "valid_from",
            StructType(
                [
                    StructField("scale", IntegerType(), False),
                    StructField("value", StringType(), False),
                ]
            ),
        ),
        StructField(
            "valid_to",
            StructType(
                [
                    StructField("scale", IntegerType(), False),
                    StructField("value", StringType(), False),
                ]
            ),
        ),
        StructField(
            "units_purchased",
            StructType(
                [
                    StructField("scale", IntegerType(), False),
                    StructField("value", StringType(), False),
                ]
            ),
        ),
        StructField("loyalty_segment", StringType(), False),
    ]
)


@dlt.table(
    comment="Load data to customers cleansed table",
    table_properties={"pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def customers_cleansed():
    return (
        dlt.read_stream("customers_raw")
        #         spark.readStream.format("delta").table("retail_org.customers_raw")
        #          spark.read.format("delta").table("retail_org.customers_raw")
        .select(get_json_object(col("value"), "$.payload.after").alias("row"))
        .select(from_json(col("row"), customers_schema).alias("row"))
        .select("row.*")
    )


# customers_cleansed().filter("valid_to is not null").display()

# COMMAND ----------

# product_id;product_category;product_name;sales_price;EAN13;EAN5;product_unit

products_schema = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_category", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField(
            "sales_price",
            StructType(
                [
                    StructField("scale", IntegerType(), False),
                    StructField("value", StringType(), False),
                ]
            ),
            False,
        ),
        StructField("ean13", DoubleType(), False),
        StructField("ean5", StringType(), False),
        StructField("product_unit", StringType(), False),
    ]
)


@dlt.table(
    comment="Load data to a products cleansed table",
    table_properties={"pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def products_cleansed():
    return (
        dlt.read_stream("products_raw")
        #         spark.readStream.format("delta").table("retail_org.products_raw")
        #         spark.read.format("delta").table("retail_org.products_raw")
        .select(get_json_object(col("value"), "$.payload.after").alias("row"))
        .select(from_json(col("row"), products_schema).alias("row"))
        .select("row.*")
    )


# products_cleansed().display()

# COMMAND ----------

# CREATE LIVE TABLE dim_products (
#   product_key BIGINT GENERATED ALWAYS AS identity,
#   product_id STRING,
#   product_category STRING,
#   product_name STRING,
#   sales_price STRUCT<scale: INT, value: STRING>,
#   ean13 DOUBLE,
#   ean5 STRING,
#   product_unit STRING
# ) COMMENT "Products dimension." TBLPROPERTIES ("quality" = "gold") AS
# SELECT
#   *
# FROM
#   retail_org.products_silver


@dlt.table(
    schema="""
         product_key BIGINT GENERATED ALWAYS AS identity,
         product_id STRING,
         product_category STRING,
         product_name STRING,
         sales_price STRUCT<scale: INT, value: STRING>,
         ean13 DOUBLE,
         ean5 STRING,
         product_unit STRING    
    """,
    comment="Load data to products dimension table",
    table_properties={"quality": "gold", "pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def dim_products():
    return dlt.read_stream("products_cleansed")

# COMMAND ----------

# CREATE LIVE TABLE dim_customers (
#  customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
#  customer_id BIGINT,
#   tax_id STRING,
#   tax_code STRING,
#   customer_name STRING,
#   state STRING,
#   city STRING,
#   postcode STRING,
#   street STRING,
#   number STRING,
#   unit STRING,
#   region STRING,
#   district STRING,
#   lon DECIMAL(10,6),
#   lat DECIMAL(10,6),
#   ship_to_address STRING,
#   valid_from STRUCT<scale: INT, value: STRING>,
#   valid_to STRUCT<scale: INT, value: STRING>,
#   units_purchased STRUCT<scale: INT, value: STRING>,
#   loyalty_segment STRING)
#   COMMENT "Customer dimension." TBLPROPERTIES ("quality" = "gold") AS
#   SELECT * FROM retail_org.customers_silver


@dlt.table(
    schema="""
          customer_key BIGINT GENERATED ALWAYS AS IDENTITY, 
          customer_id BIGINT,
          tax_id STRING,
          tax_code STRING,
          customer_name STRING,
          state STRING,
          city STRING,
          postcode STRING,
          street STRING,
          number STRING,
          unit STRING,
          region STRING,
          district STRING,
          lon DECIMAL(10,6),
          lat DECIMAL(10,6),
          ship_to_address STRING,
          valid_from STRUCT<scale: INT, value: STRING>,
          valid_to STRUCT<scale: INT, value: STRING>,
          units_purchased STRUCT<scale: INT, value: STRING>,
          loyalty_segment STRING
    """,
    comment="Load data to customers dimension table",
    table_properties={"quality": "gold", "pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def dim_customers():
    return dlt.read_stream("customers_cleansed")

# COMMAND ----------

# CREATE STREAMING LIVE TABLE fact_sales_orders
# -- CREATE OR REPLACE TEMPORARY VIEW fact_sales_orders
# AS
# SELECT s.order_number, c.customer_key, p.product_key, cast(s.order_datetime as date) order_date, s.unit_price, s.quantity, (s.unit_price * s.quantity) AS total_price   FROM retail_org.sales_orders_silver s
# INNER JOIN retail_org.dim_products p ON s.product_id = p.product_id
# INNER JOIN retail_org.dim_customers c ON s.customer_id = c.customer_id;


@dlt.table(
    comment="load data to sales orders fact table",
    table_properties={"quality": "gold", "pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def fact_sales_orders():
    s = dlt.read_stream("sales_orders_cleansed").alias("s")
    p = dlt.read_stream("dim_products").alias("p")
    c = dlt.read_stream("dim_customers").alias("c")
    return (
        s.join(p, s.product_id == p.product_id, "inner")
        .join(c, s.customer_id == c.customer_id, "inner")
        .select(
            "s.order_number",
            "c.customer_key",
            "p.product_key",
            col("s.order_datetime").cast("date"),
            "s.unit_price",
            "s.quantity",
            expr("s.unit_price * s.quantity").alias("total_price"),
        )
    )

# COMMAND ----------

# CREATE LIVE TABLE customer_sales
# -- CREATE TEMPORARY VIEW customer_sales
# AS
# SELECT customer_key, product_key, sum(quantity) total_quantity, sum(total_price) sale_amount
# FROM sales_orders_fact_tmp
# GROUP BY customer_key, product_key


@dlt.table(
    comment="load data to customer sales fact table",
    table_properties={"quality": "gold", "pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def fact_customer_sales():
    s = dlt.read("sales_orders_cleansed").alias("s")
    p = dlt.read("dim_products").alias("p")
    c = dlt.read("dim_customers").alias("c")
    return (
        s.join(p, s.product_id == p.product_id, "inner")
        .join(c, s.customer_id == c.customer_id, "inner")
        .groupBy("s.product_id", "c.customer_id")
        .agg(
            sum("quantity").alias("total_quantity"),
            sum(expr("s.unit_price * s.quantity")).alias("sale_amount"),
        )
    )

# COMMAND ----------

# MAGIC %sql --DESCRIBE TABLE retail_org.fact_sales_orders
# MAGIC --SHOW CREATE TABLE retail_org.fact_sales_orders
# MAGIC -- SELECT * FROM retail_org.fact_sales_orders
# MAGIC -- SELECT * FROM retail_org.fact_customer_sales
# MAGIC -- SELECT * FROM retail_org.dim_products
# MAGIC -- SELECT * FROM retail_org.dim_customers
# MAGIC -- SELECT * FROM retail_org.customers_raw
