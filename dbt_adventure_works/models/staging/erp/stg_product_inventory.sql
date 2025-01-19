with
    source_productinventory as (
        select
            cast(LOCATIONID as int) as pk_location_id
            , cast(PRODUCTID as int) as pk_product_id
            , coalesce(nullif(trim(cast(SHELF as string)), ''), 'N/A') as shelf
            , cast(BIN as int) as bin
            , cast(QUANTITY as int) as quantity
            , cast(MODIFIEDDATE as date) as modified_date_product
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_product
        from {{source("erp_production","productinventory")}}
    )
select *
from source_productinventory