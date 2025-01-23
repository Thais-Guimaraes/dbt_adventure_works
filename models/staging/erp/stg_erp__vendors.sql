with
    source_vendors as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_business_entity_vendors_id
            , coalesce(nullif(trim(cast(ACCOUNTNUMBER as string)), ''), 'N/A') as account_number
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_vendor
            , cast(CREDITRATING as int) as credit_rating
	        , cast(PREFERREDVENDORSTATUS as boolean) as vendor_status
	        , cast(ACTIVEFLAG as boolean) as active
            , cast(MODIFIEDDATE as date) as modified_date_vendor
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_vendor
	    from {{source("erp_purchasing","vendor")}}
    )
select *
from source_vendors