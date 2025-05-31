SELECT 
  C.country,
  SUM(quantity) AS total_quantities
FROM finance_db.raw.stg_customers C
INNER JOIN finance_db.raw.stg_orders O
ON C.customer_id= O.customer_id
INNER JOIN finance_db.raw.stg_order_items OI
ON O.order_id = OI.order_id
GROUP BY 1