with
    source_sales_territory as (
        select
            cast(TERRITORYID as int) as pk_territory_id
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_territory
            , coalesce(nullif(trim(cast(COUNTRYREGIONCODE as string)), ''), 'N/A') as country_region_code
            , coalesce(nullif(trim(cast("GROUP" as string)), ''), 'N/A') as group_territory
            , cast(SALESYTD as numeric(18,4)) as sales_ytd
            , cast(SALESLASTYEAR as numeric(18,4)) as sales_last_year
	        , cast(COSTYTD as numeric(18,4)) as cost_ytd
            , cast(COSTLASTYEAR as numeric(18,4)) as cost_last_year
            , cast(MODIFIEDDATE as date) as modified_date_territory
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_territory
        from {{source("erp_sales","salesterritory")}}
    )
select *
from source_sales_territory