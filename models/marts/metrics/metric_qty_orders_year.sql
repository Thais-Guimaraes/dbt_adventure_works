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
    d.year  -- ano da dimensão de datas
    , count(distinct f.invoice_number) as qty_orders -- contagem distinta de pedidos
from fact_data f
join dim_data d 
    on f.order_date = d.date -- relaciona a data do pedido com a tabela de datas
group by d.year
order by d.year
