with
    source_currencies as (
        select
            trim(cast(CURRENCYCODE as string)) as pk_currency
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_currency
            , cast(MODIFIEDDATE as date) as modified_date_currency
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_currency
        from {{source("erp_sales","currency")}}
    )
select *
from source_currencies