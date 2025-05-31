WITH CTE AS (
      SELECT 
            C.customer_id,
            DATEDIFF(day, max(O.order_date), GETDATE()) AS Recency_Score,
            COUNT(OI.order_id) AS Frequency_Score,
            SUM(OI.total_price) AS Monetory_Score
      FROM {{ ref('stg_customers') }} C
      INNER JOIN {{ ref('stg_orders') }} O
      ON C.customer_id = O.customer_id
      INNER JOIN {{ ref('stg_order_items') }} OI
      ON O.order_id = OI.order_id
      GROUP BY 1
),
RFM_Scores AS (
      SELECT 
            customer_id,
            NTILE(5) OVER (ORDER BY Recency_Score DESC) AS R,
            NTILE(5) OVER (ORDER BY Frequency_Score ASC) AS F,
            NTILE(5) OVER (ORDER BY Monetory_Score ASC) AS M,
            (NTILE(5) OVER (ORDER BY Recency_Score DESC) + 
             NTILE(5) OVER (ORDER BY Frequency_Score ASC) + 
             NTILE(5) OVER (ORDER BY Monetory_Score ASC)) / 3 AS RFM_Score
      FROM CTE
)

SELECT 
   customer_id,
   CASE 
      WHEN RFM_Score <= 2 THEN 'Lost Customers' 
      WHEN RFM_Score = 3 THEN 'Need Attention' 
      ELSE 'VIP Customers'
   END AS customer_segment
FROM RFM_Scores