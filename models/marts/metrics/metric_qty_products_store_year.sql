with sales_data as (
    select 
        d.year,  
        f.fk_product_id,  
        p.nm_product, 
        c.fk_store_id as store_id,  
        c.nm_store as store_name,  
        l.nm_state_province as region_name, 
        f.order_qty  
    from {{ ref('fact_sales') }} f  
    join {{ ref('dim_dates') }} d 
        on f.order_date = d.date  
    join {{ ref('dim_products') }} p 
        on f.fk_product_id = p.pk_product_id  
    join {{ ref('dim_customers') }} c
        on f.fk_customer_id = c.pk_customer_id  
    join {{ ref('dim_locations') }} l
        on c.fk_territory_id = l.pk_territory_id  
)
select 
    year,  
    fk_product_id,  
    nm_product, 
    store_id, 
    store_name,   
    region_name, 
    sum(order_qty) as qty_sold  
from sales_data  
group by year, fk_product_id, nm_product, store_id, store_name, region_name  
order by year, store_id, fk_product_id  
