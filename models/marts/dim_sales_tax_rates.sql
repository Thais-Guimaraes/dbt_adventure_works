with
    sales_tax_rates as (
        select *
        from {{ ref('stg_erp__sales_tax_rates') }}
    )
    , state_provinces as (
        select *
        from {{ ref('stg_erp__state_provinces') }}
    )
    , joined as (
        select
            state_provinces.pk_state_province_id
            , sales_tax_rates.fk_state_province
            , sales_tax_rates.tax_type
            , sales_tax_rates.tax_rate
            , sales_tax_rates.nm_sales_tax_rate
            , sales_tax_rates.modified_date_tax_rate
            , sales_tax_rates.date_key_tax_rate
            , state_provinces.modified_date_state_province
            , state_provinces.date_key_state_province
            , concat(state_provinces.pk_state_province_id, '-', sales_tax_rates.tax_type, '-', sales_tax_rates.tax_rate) as pk_sales_tax_rate_id
        from sales_tax_rates
        left join state_provinces on sales_tax_rates.fk_state_province = state_provinces.pk_state_province_id
    )
select * 
from joined