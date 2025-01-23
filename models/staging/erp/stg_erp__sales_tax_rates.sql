with
    source_salestaxrates as (
        select
            cast(SALESTAXRATEID as int) as pk_sales_tax_id
            , cast(STATEPROVINCEID as int) as fk_state_province
            , cast(TAXTYPE as int) as tax_type
            , cast(TAXRATE as numeric(18,2)) as tax_rate
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_sales_tax_rate
            , cast(MODIFIEDDATE as date) as modified_date_tax_rate
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_tax_rate
        from {{source("erp_sales","salestaxrate")}}
    )
select *
from source_salestaxrates