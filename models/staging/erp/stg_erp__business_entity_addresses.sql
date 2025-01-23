with
    source_business_entity_addresses as (
        select
            cast(BusinessEntityID as int) as business_entity_id
            , cast(AddressID as int) as address_id
            , cast(AddressTypeID as int) as address_type_id
            , cast(ModifiedDate as date) as modified_date_business_entity_address
            , to_number(to_char(cast(ModifiedDate as date), 'YYYYMMDD')) as date_key_business_entity_address
        from {{ source("erp_person", "businessentityaddress") }}
    )
select *
from source_business_entity_addresses