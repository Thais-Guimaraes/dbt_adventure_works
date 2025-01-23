with
    source_specialofferproducts as (
        select
            cast(SPECIALOFFERID as int) as pk_fk_special_offer
            , coalesce(nullif(trim(cast(PRODUCTID as string)), ''), 'N/A') as pk_fk_product_id
            , cast(MODIFIEDDATE as date) as modified_date_special_offer_product
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_special_offer_product
        from {{source("erp_sales","specialofferproduct")}}
    )
select *
from source_specialofferproducts