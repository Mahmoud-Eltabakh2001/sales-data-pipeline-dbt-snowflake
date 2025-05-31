SELECT DISTINCT
  id AS customer_id,
  name AS customer_name,
  email,
  country
FROM finance_db.raw.customers