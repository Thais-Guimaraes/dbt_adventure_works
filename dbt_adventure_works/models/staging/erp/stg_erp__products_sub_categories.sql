with
    source_products_sub_categories as (
        select
            cast(PRODUCTSUBCATEGORYID as int) as pk_sub_category
            , cast(PRODUCTCATEGORYID as int) as fk_category
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_sub_category
            , cast(MODIFIEDDATE as date) as modified_date_sub_category
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_sub_category
        from {{source("erp_production","productsubcategory")}}
    )
select *
from source_products_sub_categories