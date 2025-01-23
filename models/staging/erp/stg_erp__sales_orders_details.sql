with
    source_salesorderdetail as (
        select
            cast(SALESORDERID as int) as pk_fk_salesorder_id
            , cast(SALESORDERDETAILID as int) as pk_sales_order_detail_id
	        , coalesce(nullif(trim(cast(CARRIERTRACKINGNUMBER as string)), ''), 'N/A') as carrier_tracking
            , cast(ORDERQTY as int) as order_qty
            , cast(PRODUCTID as int) as fk_product_id
            , cast(SPECIALOFFERID as int) as fk_special_offer
            , cast(UNITPRICE as numeric(18,4)) as unit_price
            , cast(UNITPRICEDISCOUNT as  numeric(18,2)) as unit_price_discount
            , cast(MODIFIEDDATE as date) as modified_date_sales_order_detail
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_sales_order_detail
        from {{source("erp_sales","salesorderdetail")}}
    )
select *
from source_salesorderdetail