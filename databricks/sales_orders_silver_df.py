# Databricks notebook source
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
                        StructField("price", LongType(), True),
                        StructField("quantity", LongType(), True),
                        StructField("unit", StringType(), True),
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
        StructField("promo_info", StringType(), True),
    ]
)


# @dlt.table(comment="Load the silver table",spark_conf={"pipelines.trigger.interval" : "1 seconds"})
def sales_orders_silver():
    return (
        #         dlt.read_stream("issue_transaction_bronze")
        spark.read.table("cdc_test.sales_orders_bronze")
        .select(get_json_object(col("value"), "$.payload.after").alias("row"))
        .withColumn("row", regexp_replace("row", '"\\[', "["))
        .withColumn("row", regexp_replace("row", '\\]"', "]"))
        .withColumn("row", regexp_replace("row", "\\\\", ""))
        .select(from_json(col("row"), schema).alias("row"))
        .select("row.*", "*")
    )


df = sales_orders_silver().cache()
df.display()

# COMMAND ----------

# df.display()

# json = df.collect()[0][0]
json = '[{"id": "AVpfuJ4pilAPnD_xhDyM", "qty": 3, "curr": "USD", "name": "Rony LBT-GPX555 Mini-System with Bluetooth and NFC", "unit": "pcs", "price": 993}, {"id": "AVpe6jFBilAPnD_xQxO2", "qty": 3, "curr": "USD", "name": "Aeon 71.5 x 130.9 16:9 Fixed Frame Projection Screen with CineWhite Projection Surface", "unit": "pcs", "price": 218}, {"id": "AVpfIODe1cnluZ0-eg35", "qty": 2, "curr": "USD", "name": "Cyber-shot DSC-WX220 Digital Camera (Black)", "unit": "pcs", "price": 448}]'

schema = spark.range(1).select(schema_of_json(json))
display(schema)
# print(c[0][0])

# COMMAND ----------

# print(schema[0])

# COMMAND ----------

schema = StructType([    
    StructField("ordered_products", 
                ArrayType(StructType([
                  StructField("curr", StringType(), True),
                  StructField("id", StringType(), True),
                  StructField("name", StringType(), True),
                  StructField("price", LongType(), True),
                  StructField("quantity", LongType(), True),
                  StructField("unit", StringType(), True)
                ])
                          ,True)
                ,True)
])

jsonString = '{ "ordered_products" : "[{\"id\": \"AVpfuJ4pilAPnD_xhDyM\", \"qty\": 3, \"curr\": \"USD\", \"name\": \"Rony LBT-GPX555 Mini-System with Bluetooth and NFC\", \"unit\": \"pcs\", \"price\": 993}, {\"id\": \"AVpe6jFBilAPnD_xQxO2\", \"qty\": 3, \"curr\": \"USD\", \"name\": \"Aeon 71.5 x 130.9 16:9 Fixed Frame Projection Screen with CineWhite Projection Surface\", \"unit\": \"pcs\", \"price\": 218}, {\"id\": \"AVpfIODe1cnluZ0-eg35\", \"qty\": 2, \"curr\": \"USD\", \"name\": \"Cyber-shot DSC-WX220 Digital Camera (Black)\", \"unit\": \"pcs\", \"price\": 448}]" }'

jsonStringFormated = jsonString.replace('\"','"').replace('"[','[').replace(']"',']')

jsonStringFormated = jsonString.replace('[[','[').replace(']]',']').replace('"[','[').replace(']"',']')

print(jsonStringFormated)
df=spark.createDataFrame([(1, jsonStringFormated)],["id","value"])
# df.display()
df = df.select(from_json(col("value"), schema))
df.display()
