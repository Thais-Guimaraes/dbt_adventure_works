with
    source_contact_types as (
        select
            cast(CONTACTTYPEID as int) as pk_contacttype_id
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_contact
            , cast(MODIFIEDDATE as date) as modified_date_business_entity_contacts
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_business_entity_contacts
        from {{source("erp_person","contacttype")}}
    )
select *
from source_contact_types