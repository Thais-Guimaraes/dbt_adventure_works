with
    -- associando as localizações para agrupamento por região
    location_info as (
        select
            facts.invoice_number,
            facts.gross_value,
            facts.fk_bill_address_id,
            facts.sk_sales,
            --dim_locations.city,  -- considerando a cidade como região
            dim_locations.nm_state_province as region -- ou use o campo de estado/província para representar a região
        from {{ ref('fact_sales') }} as facts
        left join {{ ref('dim_locations') }} as dim_locations
            on facts.fk_bill_address_id = dim_locations.pk_address_id
    )

    -- calculando o faturamento bruto total e número de pedidos por região
    , ticket_calculations as (
        select
            
            region,
            sum(gross_value) as total_gross_value,  -- faturamento bruto total por região
            count(distinct invoice_number) as num_orders  -- número de pedidos distintos por região
        from location_info
        group by region
    )

    -- calculando o ticket médio por região
    select
        region,
        (total_gross_value) / num_orders as ticket_medio_por_regiao
    from ticket_calculations