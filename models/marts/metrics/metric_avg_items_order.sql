with 
    itemcounts as (
        select 
            invoice_number
            , count(*) as itemscount
        from {{ ref('fact_sales') }}
        group by invoice_number
)
select 
    avg(itemscount) as avg_item_order
from itemcounts