with
    source_sales_reason as (
        select
            cast(SALESREASONID as int) as pk_sales_reason_id
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_sales_reason
            , coalesce(nullif(trim(cast(REASONTYPE as string)), ''), 'N/A') as reason_type
            , cast(MODIFIEDDATE as date) as modified_date_sales_reason
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_sales_reason
        from {{source("erp_sales","salesreason")}}
    )
select *
from source_sales_reason