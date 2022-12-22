-- Databricks notebook source
CREATE LIVE TABLE dim_products (
  product_key BIGINT GENERATED ALWAYS AS identity,
  product_id STRING,
  product_category STRING,
  product_name STRING,
  sales_price STRUCT<scale: INT, value: STRING>,
  ean13 DOUBLE,
  ean5 STRING,
  product_unit STRING
) COMMENT "Products dimension." TBLPROPERTIES ("quality" = "gold") AS
SELECT
  *
FROM
  retail_org.products_silver --SELECT * FROM retail_org.products_silver
  --SELECT * FROM retail_org.customers_silver

-- COMMAND ----------

-- MAGIC %sql --DESCRIBE TABLE retail_org.sales_orders_tmp
-- MAGIC -- SHOW CREATE TABLE retail_org.customers_silver
-- MAGIC -- SELECT * FROM retail_org.sales_orders_silver

-- COMMAND ----------

CREATE LIVE TABLE dim_customers (
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
  loyalty_segment STRING)
  COMMENT "Customer dimension." TBLPROPERTIES ("quality" = "gold") AS
  SELECT * FROM retail_org.customers_silver



-- COMMAND ----------

--SELECT * FROM retail_org.dim_customers
--SELECT * FROM retail_org.dim_products
-- SELECT * FROM retail_org.sales_orders_silver

-- COMMAND ----------

CREATE LIVE TABLE fact_sales_orders
-- CREATE OR REPLACE TEMPORARY VIEW sales_orders_fact_tmp
AS
SELECT s.order_number, c.customer_key, p.product_key, s.order_datetime, s.unit_price, s.quantity, (s.unit_price * s.quantity) AS total_price   FROM retail_org.sales_orders_silver s
INNER JOIN retail_org.dim_products p ON s.product_id = p.product_id
INNER JOIN retail_org.dim_customers c ON s.customer_id = c.customer_id;

-- SELECT * FROM sales_orders_fact

-- COMMAND ----------

SELECT * FROM retail_org.fact_sales_orders 
-- SELECT * FROM retail_org.dim_products 
-- SELECT * FROM retail_org.dim_customers 
