with
    source_sales_header_reason as (
        select
            cast(SALESORDERID as int) as pk_fk_sales_order_id
            , cast(SALESREASONID as int) as pk_fk_sales_reason_id
            , cast(MODIFIEDDATE as date) as modified_date_sales_header_reason
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_sales_header_reason
        from {{source("erp_sales","salesorderheadersalesreason")}}
    )
select *
from source_sales_header_reason