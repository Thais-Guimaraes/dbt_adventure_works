-- tempo médio entre os pedidos - nova query
-- consolida os pedidos por cliente e data
with 
    pedidos_agrupados as (
        select 
            fk_customer_id as customer_id -- coluna correspondente ao id do cliente
            , order_date
            , min(fk_sales_person_id) as fk_sales_person_id -- exemplo: se precisar de outras colunas (opcional)
            , count(*) as total_items -- exemplo: para identificar quantas linhas foram agregadas
        from {{ ref('fact_sales') }}
        group by fk_customer_id, order_date
    )
    , pedidos_com_diferenca as (
        select 
            customer_id
            , order_date
            , datediff(
            day
            , lag(order_date) over (partition by customer_id order by order_date)
            , order_date
        ) as days_between_orders
        from pedidos_agrupados
    )
-- calcula o tempo médio entre pedidos
select 
    customer_id
    , avg(days_between_orders) as avg_days_between_orders
from pedidos_com_diferenca
where days_between_orders is not null
group by customer_id
order by avg_days_between_orders
