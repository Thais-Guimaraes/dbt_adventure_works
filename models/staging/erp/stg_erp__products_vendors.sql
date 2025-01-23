with
    source_product_vendors as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_business_entity_product_vendors_id
            , cast(PRODUCTID as int) as pk_fk_product_id
            , cast(AVERAGELEADTIME as int) as avg_lead_time
            , cast(STANDARDPRICE as numeric(18,4)) as st_price
	        , cast(LASTRECEIPTCOST as numeric(18,4)) as last_receipt_cost
	        , cast(LASTRECEIPTDATE as date) as last_receipt_date
            , cast(MINORDERQTY as int) as min_order_qty
	        , cast(MAXORDERQTY as int) as max_order_qty
            , cast(ONORDERQTY as int) as order_qty
	        , coalesce(nullif(trim(cast(UNITMEASURECODE as string)), ''), 'N/A') as unit_measure_code
            , cast(MODIFIEDDATE as date) as modified_date_product_vendor
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_product_vendor
        from {{source("erp_purchasing","productvendor")}}
    )
select *
from source_product_vendors