with
    source_state_province as (
        select
            cast(STATEPROVINCEID as int) as pk_state_province_id
            , coalesce(nullif(trim(cast(STATEPROVINCECODE as string)), ''), 'N/A') as code_province
            , coalesce(nullif(trim(cast(COUNTRYREGIONCODE as string)), ''), 'N/A') as fk_country_region_code
            , cast(ISONLYSTATEPROVINCEFLAG as boolean) as is_only_state
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_state_province
            , cast(TERRITORYID as int) as fk_territory_id
            , cast(MODIFIEDDATE as date) as modified_date_state_province
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_state_province
	    from {{source("erp_person","stateprovince")}}
    )
select *
from source_state_province