with 
    pedidos as (
        select 
            invoice_number
            , sum(gross_value) as gross_value
        from {{ ref('fact_sales') }}
        group by 
            invoice_number
)
select 
    avg(gross_value) as ticketmedio
from pedidos