# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
    comment="Load data to sales_orders silver table",
    table_properties={"pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def sales_orders_silver():
    return (
        #         dlt.read_stream("sales_orders_bronze")
        spark.readStream.format("delta")
        .table("retail_org.sales_orders_bronze")
        .select(get_json_object(col("value"), "$.payload.after").alias("row"))
        .withColumn("row", regexp_replace("row", '"\\[', "["))
        .withColumn("row", regexp_replace("row", '\\]"', "]"))
        .withColumn("row", regexp_replace("row", "\\\\", ""))
        .select(from_json(col("row"), sales_orders_schema).alias("row"))
        .select("row.*")
        .withColumn("ordered_products", explode("ordered_products"))
        .withColumn("order_datetime", from_unixtime("order_datetime"))
        .withColumn("product_id", col("ordered_products").id)
    )

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
    comment="Load data to customers silver table",
    table_properties={"pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def customers_silver():
    return (
        # dlt.read_stream("customers_bronze")
        spark.readStream.format("delta")
        #          spark.read.format("delta")
        .table("retail_org.customers_bronze")
        .select(get_json_object(col("value"), "$.payload.after").alias("row"))
        .select(from_json(col("row"), customers_schema).alias("row"))
        .select("row.*")
    )


# customers_silver().filter("valid_to is not null").display()

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
    comment="Load data to a products silver table",
    table_properties={"pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def products_silver():
    return (
        # dlt.read_stream("products")
        spark.readStream.format("delta")
        .table("retail_org.products_bronze")
        #         spark.read.format("delta").table("retail_org.products_bronze")
        .select(get_json_object(col("value"), "$.payload.after").alias("row"))
        .select(from_json(col("row"), products_schema).alias("row"))
        .select("row.*")
    )


# products_silver().display()

# COMMAND ----------

# MAGIC %sql --DESCRIBE TABLE retail_org.sales_orders_tmp
# MAGIC --SHOW CREATE TABLE retail_org.sales_orders_tmp
# MAGIC -- SELECT * FROM retail_org.sales_orders_silver
# MAGIC --SELECT * FROM retail_org.products_silver
