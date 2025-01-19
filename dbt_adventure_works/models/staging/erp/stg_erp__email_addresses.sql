with
    source_email_addresses as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_businessentityid
            , cast(EMAILADDRESSID as int) as pk_email_address_id
            , coalesce(nullif(trim(cast(EMAILADDRESS as string)), ''), 'N/A') as email_address
            , cast(MODIFIEDDATE as date) as modified_date_address
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_address
        from {{source("erp_person","emailaddress")}}
    )
select *
from source_email_addresses