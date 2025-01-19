with
    source_salespeople as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_business_entity_sales_people_id
            , cast(TERRITORYID as int) as fk_territory_id
            , cast(SALESQUOTA as int) as sales_quota
            , cast(BONUS as int) as sales_bonus
            , cast(COMMISSIONPCT as numeric(5,3)) as commission_pct
            , cast(SALESYTD as numeric(18,4)) as sales_ytd
            , cast(SALESLASTYEAR as numeric(18,4)) as sales_last_year
            , cast(MODIFIEDDATE as date) as modified_date_sales_person
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_sales_person
        from {{source("erp_sales","salesperson")}}
    )
select *
from source_salespeople