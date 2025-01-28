WITH 
    max_date AS (
        -- Vou considerar a data atual como sendo a última data de compra + 1, porque o dataset é ate 2014
        SELECT
            DATEADD(day, 1, MAX(order_date)) AS lastdate
        FROM {{ ref('fact_sales') }}
    ),
    customer_age_group AS (
        SELECT
            fs.fk_customer_id AS customerid,
            ag.age_group
        FROM {{ ref('fact_sales') }} fs
        JOIN {{ ref('metric_customer_age') }} ag
            ON fs.fk_customer_id = ag.customerid
    )

SELECT
    cag.age_group,
    COUNT(fs.invoice_number) AS total_orders, 
    SUM(fs.gross_value) AS total_revenue,
    AVG(fs.gross_value) AS average_ticket 
FROM {{ ref('fact_sales') }} fs
JOIN customer_age_group cag
    ON fs.fk_customer_id = cag.customerid
GROUP BY cag.age_group
ORDER BY cag.age_group