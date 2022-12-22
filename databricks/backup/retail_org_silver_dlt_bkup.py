# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType(
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
    comment="Load to a sales_orders silver table",
    schema="""
              id BIGINT GENERATED ALWAYS AS IDENTITY,              
              customer_id BIGINT,
              customer_name STRING,
              order_datetime STRING,
              order_number BIGINT,
              product_id STRING,
              ordered_products STRUCT<curr: STRING, 
                                      id: STRING,name: STRING, 
                                      price: INT, 
                                      qty: INT, 
                                      unit: STRING, 
                                      promotion_info: STRUCT<promo_id: INT, 
                                                              promo_qty: INT, 
                                                              promo_disc: DECIMAL(3,2), 
                                                              promo_item: STRING>>,
              number_of_line_items BIGINT,
              clicked_items ARRAY<ARRAY<STRING>>,
              promo_info ARRAY<STRUCT<promo_id: INT, 
                                      promo_qty: INT, 
                                      promo_disc: DECIMAL(3,2), 
                                      promo_item: STRING>>              
          """,
    table_properties={"pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=True,
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
        .select(from_json(col("row"), schema).alias("row"))
        .select("row.*")
        .withColumn("ordered_products", explode("ordered_products"))
        .withColumn("order_datetime", from_unixtime("order_datetime"))
        .withColumn("product_id", col("ordered_products").id)
    )

# COMMAND ----------

# MAGIC %sql --DESCRIBE TABLE retail_org.sales_orders_tmp
# MAGIC --SHOW CREATE TABLE retail_org.sales_orders_tmp
# MAGIC -- SELECT * FROM retail_org.sales_orders_silver
