with
    source_products_categories as (
        select
            cast(PRODUCTCATEGORYID as int) as pk_category
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_category
            , cast(MODIFIEDDATE as date) as modified_date_category
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_category
        from {{source("erp_production","productcategory")}}
    )
select * 
from source_products_categories