with
    source_businessentity as (
        select
            cast(BUSINESSENTITYID as int) as pk_business_entity_contact_id
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_business_entity_contacts
        from {{source("erp_person","businessentity")}}
    )
select *
from source_businessentity