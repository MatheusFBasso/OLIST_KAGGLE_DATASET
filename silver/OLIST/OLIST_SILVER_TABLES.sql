CREATE CATALOG IF NOT EXISTS datum;

USE CATALOG datum;

CREATE DATABASE IF NOT EXISTS silver;

USE DATABASE silver;

CREATE TABLE IF NOT EXISTS olist_customers
(
  customer_id              STRING NOT NULL,
  customer_unique_id       STRING NOT NULL,
  customer_zip_code_prefix STRING,
  customer_city            STRING,
  customer_state           STRING,
  date_ref_carga           DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_customers';

CREATE TABLE IF NOT EXISTS o_geolocation
(
  geolocation_zip_code_prefix STRING NOT NULL,
  geolocation_lat             DOUBLE,
  geolocation_lng             DOUBLE,
  geolocation_city            STRING, 
  geolocation_state           STRING,
  date_ref_carga              DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_geolocation';

CREATE TABLE IF NOT EXISTS olist_order_items
(
  order_id            STRING NOT NULL,
  order_item_id       INTEGER NOT NULL,
  product_id          STRING,
  seller_id           STRING,
  shipping_limit_date TIMESTAMP,
  price               DECIMAL(10,4),
  freight_value       DECIMAL(10,4),
  date_ref_carga      DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_order_items';

CREATE TABLE IF NOT EXISTS olist_payments
(
  order_id              STRING NOT NULL,
  payment_sequential    INTEGER,
  payment_type          STRING,
  payment_installments  INTEGER,
  payment_value         DECIMAL(10,4),
  date_ref_carga        DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_payments';

CREATE TABLE IF NOT EXISTS olist_order_reviews
(
  review_id               STRING NOT NULL,
  order_id                STRING NOT NULL,
  review_score            INTEGER,
  review_comment_title    STRING,
  review_comment_message  STRING,
  review_creation_date    TIMESTAMP,
  review_answer_timestamp TIMESTAMP,
  date_ref_carga          DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_order_reviews';

CREATE TABLE IF NOT EXISTS olist_orders
(
  order_id                      STRING NOT NULL,
  customer_id                   STRING NOT NULL,
  order_status                  STRING,
  order_purchase_timestamp      TIMESTAMP,
  order_approved_at             TIMESTAMP,
  order_delivered_carrier_date  TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP,
  date_ref_carga                DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_orders';

CREATE TABLE IF NOT EXISTS olist_orders_datediff
(
  order_id                STRING NOT NULL,
  customer_id             STRING NOT NULL,
  delivery_diff_promissed INTEGER,
  date_ref_carga          DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_orders_datediff';

CREATE TABLE IF NOT EXISTS olist_category_name_translation
(
  product_category_name         STRING NOT NULL,
  product_category_name_english STRING NOT NULL,
  date_ref_carga                DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_category_name_translation';

CREATE TABLE IF NOT EXISTS olist_products
(
  product_id                 STRING NOT NULL,
  product_category_name      STRING,
  product_name_lenght        INTEGER,
  product_description_lenght INTEGER,
  product_weight_g           INTEGER,
  product_length_cm          INTEGER,
  product_height_cm          INTEGER,
  product_width_cm           INTEGER,
  date_ref_carga             DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_products';

CREATE TABLE IF NOT EXISTS olist_sellers
(
  seller_id              STRING NOT NULL,
  seller_zip_code_prefix STRING,
  seller_city            STRING,
  seller_state           STRING,
  date_ref_carga         DATE
)
USING DELTA
LOCATION 'abfss://unity-datum@datumunity.dfs.core.windows.net/silver/olist_sellers';

SHOW TABLES