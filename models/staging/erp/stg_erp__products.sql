with
    source_products as (
        select
            cast(PRODUCTID as int) as pk_product_id
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_product
            , coalesce(nullif(trim(cast(PRODUCTNUMBER as string)), ''), 'N/A') as code_product
	        , cast(MAKEFLAG as boolean) as make_flag
	        , cast(FINISHEDGOODSFLAG as boolean) as finished_goods_flag
            , cast(SAFETYSTOCKLEVEL as int) as safety_stock_level
	        , cast(REORDERPOINT as int) as reorder_point
            , cast(STANDARDCOST as numeric(18,4)) as standard_cost
            , cast(LISTPRICE as numeric(18,4)) as list_price
            , cast(WEIGHT as numeric(18,2)) as weight
            , cast(DAYSTOMANUFACTURE as int) as days_manufacture
	        , coalesce(nullif(trim(cast(PRODUCTLINE as string)), ''), 'N/A') as product_line
            , coalesce(nullif(trim(cast(CLASS as string)), ''), 'N/A') as class
            , coalesce(nullif(trim(cast(STYLE as string)), ''), 'N/A') as style
            , cast(PRODUCTSUBCATEGORYID as int) as fk_sub_category
            , cast(SELLSTARTDATE as date) as sell_start_date
            , cast(SELLENDDATE as date) as sell_end_date
            , to_date(CAST(DISCONTINUEDDATE as string), 'YYYYMMDD') as discontinued_date
            , cast(MODIFIEDDATE as date) as modified_date_product
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_product
        from {{source("erp_production","product")}}
    )
select *
from source_products