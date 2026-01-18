-- Build Revenue Table
-- Creates daily revenue for all products in January 2025
-- Partitioned by date, clustered by sku_id for query performance

CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.revenue`
PARTITION BY date_id
CLUSTER BY sku_id
AS
SELECT
    p.sku_id,
    d AS date_id,
    p.price,
    COALESCE(s.total_sales, 0) AS sales,
    ROUND(p.price * COALESCE(s.total_sales, 0), 2) AS revenue
FROM `{project_id}.{dataset_id}.product` p
CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(
    DATE '2025-01-01', 
    DATE '2025-01-31'
)) AS d
LEFT JOIN (
    SELECT 
        sku_id, 
        DATE(orderdate_utc) AS date_id, 
        SUM(sales) AS total_sales
    FROM `{project_id}.{dataset_id}.sales`
    GROUP BY 1, 2
) s ON p.sku_id = s.sku_id AND d = s.date_id
