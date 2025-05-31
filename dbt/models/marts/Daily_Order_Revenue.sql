SELECT 
  O.order_date,
  SUM(total_price) AS total_price
FROM {{ ref('stg_orders') }} O 
LEFT JOIN {{ ref('stg_order_items') }} OI
ON O.order_id = OI.order_id
GROUP BY 1

