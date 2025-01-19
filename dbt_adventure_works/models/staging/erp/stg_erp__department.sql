with
    source_department as (
        select
            cast(DEPARTMENTID as int) as pk_fk_business_entity_contact_id
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_department
             , coalesce(nullif(trim(cast(GROUPNAME as string)), ''), 'N/A') as nm_group
            , cast(MODIFIEDDATE as date) as modified_date_business_entity_contacts
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_business_entity_contacts
        from {{source("erp_humanresources","department")}}
    )
select *
from source_department