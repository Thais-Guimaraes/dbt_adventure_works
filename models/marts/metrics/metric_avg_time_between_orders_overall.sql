-- tempo médio geral entre pedidos
with 
    pedidos_agrupados as (
        select 
            fk_customer_id as customer_id -- coluna correspondente ao id do cliente
            , order_date
            , count(*) as total_items -- exemplo: para identificar quantas linhas foram agregadas
        from {{ ref('fact_sales') }}
        group by fk_customer_id, order_date
    )

    , pedidos_com_diferenca as (
        select 
            customer_id
            , order_date
            , datediff(
            day,
            lag(order_date) over (partition by customer_id order by order_date)
            , order_date
        ) as days_between_orders
        from pedidos_agrupados
    )
select 
    avg(days_between_orders) as avg_days_between_all_orders
from pedidos_com_diferenca
where days_between_orders is not null