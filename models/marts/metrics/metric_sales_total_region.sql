----total de vendas por regiao
with 
    sales as (
        select
            facts.invoice_number
            , facts.fk_bill_address_id
            , facts.gross_value 
        from {{ ref('fact_sales') }} as facts
    )
    , locations as (
        select
            loc.pk_address_id
            , loc.nm_state_province as regionname
        from {{ ref('dim_locations') }} as loc
    )

select
    loc.regionname as regiao
    , sum(gross_value) as valortotalvendas
from sales s
join locations loc
    on s.fk_bill_address_id = loc.pk_address_id
group by loc.regionname
order by valortotalvendas desc
