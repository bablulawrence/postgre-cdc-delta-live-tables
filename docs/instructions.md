Environment
==================

1.Provision windows VM(Use azure image)
2.Install WSL

3. Install Postgre SQL
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo service postgresql start
sudo -u postgres psql

sudo -i -u postgres
psql
\q
\dt /*list tables*/
\d table anme  /*deplay schema*/



Loading sales_order json file from retail_org databricks datasets
-------------------------------------------------------------------
\Connect Retail 
CREATE TABLE temp (data jsonb);
\COPY temp(data) FROM '/home/blawrence/ws/sales_orders.json';
SELECT data->>'customer_id', data->>'customer_name', data->>'clicked_items' FROM temp;



CREATE TABLE sales_order (
	order_number integer,
	customer_id integer,
	order_datetime text,	
	customer_name text,
	clicked_items json,		
	number_of_line_items smallint,	
	ordered_products json,
	promo_info json,
	PRIMARY KEY(order_number, customer_id, order_datetime)
);

INSERT INTO sales_order (order_number, customer_id, customer_name, clicked_items, number_of_line_items,
						order_datetime, ordered_products, promo_info)
SELECT (data->>'order_number')::integer, (data->>'customer_id')::integer, (data->>'customer_name')::text, 
	(data->>'clicked_items')::json, (data->>'number_of_line_items')::smallint, 
	(data->>'order_datetime')::text, (data->>'ordered_products')::json, (data->>'promo_info')::json
FROM temp;

------------------
\Connect Retail 
CREATE TABLE temp_customers (data csv);
\COPY temp(data) FROM '/home/blawrence/ws/customers.csv';
SELECT data->>'customer_id', data->>'customer_name', data->>'ship_to_address' FROM temp;


CREATE TABLE customers (
	customer_id integer NOT NULL,
	tax_id varchar(20) NULL,
	tax_code CHAR(1) NULL,
	customer_name varchar(100) NOT NULL,
	state char(2) NULL,
	city varchar(50) NULL,
	postcode varchar(20) NULL,
	street varchar(50) NULL,
	number varchar(50) NULL, 
	unit varchar(50) NULL,
	region varchar(20) NULL,
	district varchar(50)NULL,
	lon float8 NULL,
	lat float8 NULL,
	ship_to_address varchar(100) NULL,
	valid_from numeric NOT NULL,
	valid_to numeric NULL,
	units_purchased numeric NULL,
	loyalty_segment integer NULL,	
	PRIMARY KEY(customer_id, valid_from)
);

\COPY customers FROM '/home/blawrence/ws/customers.csv' WITH (FORMAT csv, HEADER true, NULL '')

CREATE TABLE products (
	product_id char(20) NOT NULL,
	product_category varchar(20) NOT NULL,
	product_name varchar(120) NOT NULL,
	sales_price numeric NULL, 
	EAN13 float8 NULL,
	EAN5 char(5) NULL,
	product_unit char(3) NULL,	
	PRIMARY KEY(product_id)
);

\COPY products FROM '/home/blawrence/ws/products.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL '')

ALTER DATABASE "Retail" RENAME TO retail_org;

pg_dump retail_org > retail_org.sql
(default location /var/lib/postgresql)

psql retail_org1 < retail_org.sql

Databricks
===================

databricks secrets create-scope --scope adlsgen2 --initial-manage-principal users
databricks secrets put --scope adlsgen2 --key adlsmountkey
