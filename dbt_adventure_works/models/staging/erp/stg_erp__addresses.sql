with
    source_addresses as (
        select
            cast(ADDRESSID as int) as pk_address_id
            , coalesce(nullif(trim(cast(CITY as string)), ''), 'N/A') as city
            , cast(STATEPROVINCEID as int) as fk_state_province_id
            , cast(MODIFIEDDATE as date) as modified_date_address
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_address
        from {{source("erp_person","address")}}
    )
select *
from source_addresses