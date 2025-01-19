with
    source_stores as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_businessentityid
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_store
            , cast(MODIFIEDDATE as date) as modified_date_store
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_store
        from {{source("erp_sales","store")}}
    )
select *
from source_stores