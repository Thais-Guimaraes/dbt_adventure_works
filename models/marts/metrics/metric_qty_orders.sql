with fact_data as (
    select *
    from {{ ref('fact_sales') }}  
)
select 
    count(distinct invoice_number) as qty_orders  -- Count Orders
FROM fact_data