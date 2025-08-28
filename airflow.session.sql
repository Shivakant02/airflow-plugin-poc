-- Get 100 records from the airflow-poc table
SELECT 
  *
FROM "airflow-poc"
ORDER BY created_at DESC
LIMIT 100;