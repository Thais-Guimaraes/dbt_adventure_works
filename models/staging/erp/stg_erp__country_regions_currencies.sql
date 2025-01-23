with
    source_country_regions_currencies as (
        select
            trim(cast(COUNTRYREGIONCODE as string)) as pk_country_region
            , coalesce(nullif(trim(cast(CURRENCYCODE as string)), ''), 'N/A') as currency_code
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_country_region
 	    from {{source("erp_sales","countryregioncurrency")}}
    )
select *
from source_country_regions_currencies