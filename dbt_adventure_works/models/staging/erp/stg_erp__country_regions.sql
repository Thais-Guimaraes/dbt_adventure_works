with
    source_country_region as (
        select
            trim(cast(COUNTRYREGIONCODE as string)) as pk_country_region
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_country
            , cast(MODIFIEDDATE as date) as modified_date_country_region
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_country_region
 	    from {{source("erp_person","countryregion")}}
    )
select *
from source_country_region