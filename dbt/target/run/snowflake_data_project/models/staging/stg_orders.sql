
  create or replace   view finance_db.raw.stg_orders
  
   as (
    SELECT DISTINCT
  id AS order_id,
  customer_id,
  order_date,
  YEAR(order_date) AS year,
  MONTH(order_date) AS month,
  DAY(order_date) AS day,
  status AS order_status
FROM finance_db.raw.orders
  );

