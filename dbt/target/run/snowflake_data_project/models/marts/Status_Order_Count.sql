
  
    

        create or replace transient table finance_db.raw.Status_Order_Count
         as
        (SELECT 
  O.order_status,
  COUNT(OI.item_id) AS "No of Orders"
FROM finance_db.raw.stg_orders O 
LEFT JOIN finance_db.raw.stg_order_items OI
ON O.order_id = OI.order_id
GROUP BY 1
        );
      
  