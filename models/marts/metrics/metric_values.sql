with 
    -- detalhamento dos itens de vendas e suas métricas
    joined as (
        select 
            f.invoice_number,
            f.fk_product_id,
            f.fk_sales_person_id,
            f.fk_customer_id,
            f.order_qty,
            f.unit_price,
            f.unit_price_discount,
            f.tax_amt,
            f.freight,
            f.sub_total,
            f.weight,
            f.order_date,
            f.status,
            f.ship_date,
            f.purchase_order,
            f.account_number,
            f.modified_date_sales_order_header,
            f.date_key_sales_order_header,
            f.fk_bill_address_id,
            -- você pode adicionar outros campos de `fact_sales` conforme necessário
            -- exemplo: f.fk_credit_card_id, f.fk_currency_rate_id, etc.
            sp.hire_date, -- dados do vendedor
            c.nm_customer -- nome do cliente
        from {{ ref('fact_sales') }} as f
        left join {{ ref('dim_sales_people') }} as sp on f.fk_sales_person_id = sp.pk_fk_business_entity_sales_people_id
        left join {{ ref('dim_customers') }} as c on f.fk_customer_id = c.pk_customer_id
    ),
    
    -- contagem das linhas por fatura (invoice_number)
    line_counts as (
        select 
            invoice_number,
            count(*) as line_count
        from joined
        group by invoice_number
    ),
    
    -- calculo das métricas
    metrics as (
        select
            j.invoice_number,
            -- senioridade do vendedor
            cast((j.hire_date - j.order_date) / 365 as numeric(18, 2)) as seniority_years,
            
            -- distribuição do frete proporcional à quantidade ou peso
            case
                -- se algum produto tem peso nulo, distribui o frete proporcional à quantidade
                when exists (
                    select 1
                    from joined as j2
                    where j2.invoice_number = j.invoice_number
                    and j2.weight is null
                ) then 
                    -- distribui o frete proporcional à quantidade
                    j.freight / lc.line_count
                else 
                    -- se todos os produtos têm peso, distribui o frete proporcional ao peso
                    j.freight * (j.order_qty * j.weight) / 
                    (select sum(coalesce(order_qty * weight, 0)) 
                     from joined as j3 
                     where j3.invoice_number = j.invoice_number)
            end as shared_freight,
            
            -- percentual de peso do produto na fatura
            case
                when j.weight is not null then (j.order_qty * j.weight) / wt.total_weight * 100.0
                else null
            end as percent_weight,
            
            -- valor bruto da fatura
            j.order_qty * j.unit_price as gross_value,
            
            -- imposto distribuído por linha
            (j.tax_amt / lc.line_count) as distributed_tax_amt,
            
            -- valor líquido (após imposto e desconto)
            ((j.order_qty * j.unit_price) + (j.tax_amt / lc.line_count) + shared_freight - (j.order_qty * j.unit_price * j.unit_price_discount)) as net_value
        from joined as j
        left join line_counts as lc on j.invoice_number = lc.invoice_number
        left join {{ ref("weight_totals") }} as wt on j.invoice_number = wt.salesorder_id
    )

-- seleção final das métricas calculadas
select 
    invoice_number,
    gross_value,
    net_value,
    shared_freight,
    percent_weight,
    distributed_tax_amt
from metrics
