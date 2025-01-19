with
    source_shopping_cart_item as (
        select
            cast(SHOPPINGCARTITEMID as int) as pk_cart_item_id
            , cast(SHOPPINGCARTID as int) as fk_cart_id
            , cast(QUANTITY as int) as cart_qty
            , cast(PRODUCTID as int) as fk_product_id
            , cast(DATECREATED as date) as date_created_cart_item
            , cast(MODIFIEDDATE as date) as modified_date_shopping_cart_item
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_shopping_cart_item
	    from {{source("erp_sales","shoppingcartitem")}}
    )
select *
from source_shopping_cart_item