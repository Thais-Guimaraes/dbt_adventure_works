WITH 
    fact_data AS (
        SELECT *
        FROM {{ ref('fact_sales') }} 
    ),
    dim_data AS (
        SELECT *
        FROM {{ ref('dim_dates') }}
    )

SELECT 
    d.year,  
    f.fk_product_id,  
    p.nm_product, 
    count(distinct f.invoice_number) AS qty_orders  
FROM fact_data f
JOIN dim_data d 
    ON f.order_date = d.date  
JOIN {{ ref('dim_products') }} p 
    ON f.fk_product_id = p.pk_product_id  
GROUP BY d.year, f.fk_product_id, p.nm_product  -- Agrupa por ano, ID do produto e nome do produto
ORDER BY d.year, f.fk_product_id  -- Ordena por ano e ID do produto