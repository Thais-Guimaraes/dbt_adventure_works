-- quantidade de pedidos por região
with 
    fact_data as (
        select *
        from {{ ref('fact_sales') }}
    )
select 
    loc.nm_state_province as regiao  
    , count(distinct(f.invoice_number)) as quantidadepedidos 
from fact_data f
join {{ ref('dim_locations') }} loc
    on f.fk_bill_address_id = loc.pk_address_id  
group by loc.nm_state_province  
order by quantidadepedidos desc
