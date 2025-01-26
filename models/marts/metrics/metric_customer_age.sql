WITH 
    max_date AS (
        -- A última data de compra (com a data + 1)
        SELECT
            DATEADD(day, 1, MAX(order_date)) AS lastdate
        FROM {{ ref('fact_sales') }}
    ),
    customers AS (
        -- Obtendo a data de nascimento dos clientes
        SELECT
            c.pk_customer_id AS customerid,
            p.date_birth AS birthdate
        FROM {{ ref('stg_erp__customers') }} AS c
        JOIN {{ ref('stg_erp__people') }} AS p
            ON c.pk_customer_id = p.pk_fk_business_entity_people_id
    ),
    customer_age AS (
        -- Calculando a idade de cada cliente com base na última data de compra
        SELECT
            c.customerid,
            DATEDIFF(year, c.birthdate, md.lastdate) AS age
        FROM customers c
        CROSS JOIN max_date md -- A última data + 1 como referência
    )
-- Agrupando os clientes em faixas etárias
SELECT
    customerid,
    CASE
        WHEN age BETWEEN 0 AND 9 THEN '0-9'
        WHEN age BETWEEN 10 AND 19 THEN '10-19'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        WHEN age BETWEEN 50 AND 59 THEN '50-59'
        WHEN age BETWEEN 60 AND 69 THEN '60-69'
        WHEN age >= 70 THEN '70+'
        ELSE 'Unknown'
    END AS age_group
FROM customer_age
ORDER BY age