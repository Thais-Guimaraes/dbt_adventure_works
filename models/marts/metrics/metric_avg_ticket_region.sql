with
    location_info as (
        select
            facts.invoice_number,
            facts.gross_value,
            facts.fk_bill_address_id,
            facts.sk_sales,
            dim_locations.nm_state_province as region 
        from {{ ref('fact_sales') }} as facts
        left join {{ ref('dim_locations') }} as dim_locations
            on facts.fk_bill_address_id = dim_locations.pk_address_id
    )

   
    , ticket_calculations as (
        select
            
            region,
            sum(gross_value) as total_gross_value,  
            count(distinct invoice_number) as num_orders  
        from location_info
        group by region
    )

    -- calculando o ticket médio por região
    select
        region,
        (total_gross_value) / num_orders as ticket_medio_por_regiao
    from ticket_calculations