WITH 
    max_date AS (
        -- A última data de compra (com a data + 1)
        SELECT
            DATEADD(day, 1, MAX(order_date)) AS lastdate
        FROM {{ ref('fact_sales') }}
    ),
    customer_age_group AS (
        -- Calculando a faixa etária de cada cliente (supondo que já tenha a tabela com essa informação)
        SELECT
            fs.fk_customer_id AS customerid,
            ag.age_group
        FROM {{ ref('fact_sales') }} fs
        JOIN {{ ref('metric_customer_age') }} ag
            ON fs.fk_customer_id = ag.customerid
    )
-- Calculando o ticket médio por faixa etária
SELECT
    cag.age_group,
    COUNT(fs.invoice_number) AS total_orders, -- Número total de pedidos
    SUM(fs.gross_value) AS total_revenue, -- Receita total
    AVG(fs.gross_value) AS average_ticket -- Ticket médio
FROM {{ ref('fact_sales') }} fs
JOIN customer_age_group cag
    ON fs.fk_customer_id = cag.customerid
GROUP BY cag.age_group
ORDER BY cag.age_group