with 
    fact_data as (
        select *
        from {{ ref('fact_sales') }}
    )
    , dim_data as (
        select *
        from {{ ref('dim_dates') }}
    )

select 
    d.year  
    , count(distinct f.invoice_number) as qty_orders 
from fact_data f
join dim_data d 
    on f.order_date = d.date 
group by d.year
order by d.year
