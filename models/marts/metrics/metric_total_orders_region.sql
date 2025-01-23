-- quantidade de pedidos por região
with 
    fact_data as (
        select *
        from {{ ref('fact_sales') }}
    )
select 
    loc.nm_state_province as regiao  -- nome do estado/província
    , count(distinct(f.invoice_number)) as quantidadepedidos  -- contagem de pedidos únicos
from fact_data f
join {{ ref('dim_locations') }} loc
    on f.fk_bill_address_id = loc.pk_address_id  -- junção com a tabela dim_locations
group by loc.nm_state_province   -- agrupamento apenas pelo nome do estado
order by quantidadepedidos desc
