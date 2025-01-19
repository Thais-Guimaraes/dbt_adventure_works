with
    source_salesterritoryhistories as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_business_entity
            , cast(TERRITORYID as int) as pk_fk_territory_id
            , cast(STARTDATE as date) as start_date
            , cast(ENDDATE as date) as end_date
            , cast(MODIFIEDDATE as date) as modified_date_territory_history
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_territory_history
        from {{source("erp_sales","salesterritoryhistory")}}
    )
select *
from source_salesterritoryhistories