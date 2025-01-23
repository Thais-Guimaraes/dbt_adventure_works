with
    products as (
        select *
        from {{ ref("stg_erp__products") }}
    )
    , facts as (
        select * 
        from {{ ref("int_orders_items") }}
    ),
    joined as (
        SELECT
            facts.pk_fk_salesorder_id AS salesorder_id,
            SUM(facts.order_qty * COALESCE(products.weight, 0)) AS total_weight
            FROM facts
            INNER JOIN products ON facts.fk_product_id = products.pk_product_id
            GROUP BY facts.pk_fk_salesorder_id
            ORDER BY facts.pk_fk_salesorder_id
    )
select * 
from joined