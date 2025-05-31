SELECT 
  O.order_status,
  COUNT(OI.item_id) AS "No of Orders"
FROM {{ ref('stg_orders') }} O 
LEFT JOIN {{ ref('stg_order_items') }} OI
ON O.order_id = OI.order_id
GROUP BY 1