CREATE OR REPLACE WAREHOUSE finance_dwh
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND=60
AUTO_RESUME=TRUE
INITIALLY_SUSPENDED=TRUE;
------------------------------------------------

CREATE OR REPLACE DATABASE finance_db;
CREATE OR REPLACE SCHEMA raw;
------------------------------------------------

CREATE OR REPLACE TABLE raw.customers(
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(100)
);

CREATE OR REPLACE TABLE raw.orders (
     id INT PRIMARY KEY,
     customer_id INT ,
     order_date DATE ,
     total_amount FLOAT ,
     status VARCHAR(50)
);


CREATE OR REPLACE TABLE raw.order_items (
     id INT PRIMARY KEY,
     order_id INT ,
     product_id INT ,
     quantity INT ,
     unit_price FLOAT
);


CREATE OR REPLACE TABLE raw.products (
     id INT PRIMARY KEY,
     name STRING,
     category STRING,
     Price FLOAT
);


----------------------------------------------------------------

CREATE OR REPLACE STAGE Sales_stage
FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);

--------------------------------------------------------------

COPY INTO raw.customers
FROM @Sales_stage/customers.csv
FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);

COPY INTO raw.orders
FROM @Sales_stage/orders.csv
FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);


COPY INTO raw.products
FROM @Sales_stage/products.csv
FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);

COPY INTO raw.order_items
FROM @Sales_stage/order_items.csv
FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);

SELECT * FROM order_items;

--------------------------------------------------------
CREATE USER dbt_user PASSWORD='dbt_password'
LOGIN_NAME='dbt_user'
DEFAULT_ROLE=ACCOUNTADMIN
MUST_CHANGE_PASSWORD=FALSE;

GRANT ROLE ACCOUNTADMIN TO USER dbt_user;

GRANT USAGE ON WAREHOUSE finance_dwh TO ROLE ACCOUNTADMIN;

GRANT USAGE ON DATABASE finance_db TO ROLE ACCOUNTADMIN;

GRANT USAGE ON SCHEMA finance_db.raw TO ROLE ACCOUNTADMIN;

GRANT SELECT , INSERT , UPDATE , DELETE  ON ALL TABLES IN SCHEMA finance_db.raw TO ROLE ACCOUNTADMIN;