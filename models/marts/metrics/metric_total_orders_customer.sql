with 
    customer_purchase_count as (
        select
            fk_customer_id as customer_id
            , count(distinct invoice_number) as total_purchases_per_customer
        from {{ ref('fact_sales') }}
        group by fk_customer_id
    )
select
    customer_id
    , total_purchases_per_customer
from customer_purchase_count
order by total_purchases_per_customer desc
