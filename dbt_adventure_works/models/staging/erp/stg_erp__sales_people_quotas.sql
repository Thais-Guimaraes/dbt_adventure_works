with
    source_salespeoplequotas as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_business_entity_people_quotas_id
            , CAST(QUOTADATE as date) as pk_quota_date
            , cast(SALESQUOTA as numeric(18,2)) as sales_quota
            , cast(MODIFIEDDATE as date) as modified_date_person_quota
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_person_quota
        from {{source("erp_sales","salespersonquotahistory")}}
    )
select *
from source_salespeoplequotas