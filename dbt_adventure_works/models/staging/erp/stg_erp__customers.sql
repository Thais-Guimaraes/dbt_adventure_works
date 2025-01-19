with
    source_customers as (
        select
            cast(CUSTOMERID as int) as pk_customer_id
            , cast(PERSONID as int) as fk_person_id_customer
            , cast(STOREID as int) as fk_store_id
            , cast(TERRITORYID as int) as fk_territory_id
            , cast(MODIFIEDDATE as date) as modified_date_customer
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_customer
	    from {{source("erp_sales","customer")}}
    )
select *
from source_customers
