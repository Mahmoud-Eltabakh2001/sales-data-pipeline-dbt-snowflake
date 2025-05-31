SELECT 
  O.order_date,
  SUM(total_price) AS total_price
FROM finance_db.raw.stg_orders O 
LEFT JOIN finance_db.raw.stg_order_items OI
ON O.order_id = OI.order_id
GROUP BY 1