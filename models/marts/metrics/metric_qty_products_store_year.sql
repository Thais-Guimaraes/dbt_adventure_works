with sales_data as (
    select 
        d.year,  
        f.fk_product_id,  
        p.nm_product, 
        c.fk_store_id as store_id,  -- usando o fk_store_id para identificar a loja
        c.nm_store as store_name,   -- nome da loja
        l.nm_state_province as region_name,  -- nome da região (província ou estado)
        f.order_qty  -- quantidade de produtos vendidos
    from {{ ref('fact_sales') }} f  -- referência ao modelo de vendas
    join {{ ref('dim_dates') }} d 
        on f.order_date = d.date  
    join {{ ref('dim_products') }} p 
        on f.fk_product_id = p.pk_product_id  
    join {{ ref('dim_customers') }} c
        on f.fk_customer_id = c.pk_customer_id  -- relacionando a tabela de vendas com a tabela de clientes (lojas)
    join {{ ref('dim_locations') }} l
        on c.fk_territory_id = l.pk_territory_id  -- relacionando o território (loja) com as localizações
)
select 
    year,  
    fk_product_id,  
    nm_product, 
    store_id, 
    store_name,   
    region_name, 
    sum(order_qty) as qty_sold  -- soma das quantidades de produtos vendidos
from sales_data  -- usando a cte para referenciar os dados de vendas
group by year, fk_product_id, nm_product, store_id, store_name, region_name  -- agrupando por ano, produto, loja e região
order by year, store_id, fk_product_id  -- ordenando por ano, id da loja (store) e id do produto
