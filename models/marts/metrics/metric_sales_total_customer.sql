with sales_data as (
    {{ calculate_gross_value('fact_sales', 'fk_customer_id') }}
)
select 
    group_key as customer_id,  
    total_sales_value
from sales_data
order by total_sales_value desc