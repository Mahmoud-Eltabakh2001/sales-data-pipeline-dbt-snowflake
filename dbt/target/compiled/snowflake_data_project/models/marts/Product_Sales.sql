SELECT 
  P.product_category,
  P.product_name,
  SUM(total_price) AS total_price
FROM finance_db.raw.stg_products P
LEFT JOIN finance_db.raw.stg_order_items OI
ON P.product_id = OI.product_id
GROUP BY 1, 2