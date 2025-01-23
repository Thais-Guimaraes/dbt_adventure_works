with
    source_productcosthistory as (
        select
            cast(PRODUCTID as int) as pk_product_id
            , cast(STARTDATE as date) as start_date
            , cast(ENDDATE as date) as end_date
            , cast(STANDARDCOST as numeric(18,4)) as standard_cost
            , cast(MODIFIEDDATE as date) as modified_date_product
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_product
        from {{source("erp_production","productcosthistory")}}
    )
select *
from source_productcosthistory