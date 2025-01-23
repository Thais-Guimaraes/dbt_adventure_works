with
    orders as (
        select *
        from {{ ref('stg_erp__sales_orders_header') }}
    )

    , orders_details as (
        select *
        from {{ ref('stg_erp__sales_orders_details') }}
    )

    , joined as (
        select
            orders_details.pk_fk_salesorder_id
            , orders_details.fk_product_id
            , orders.fk_sales_person_id 
            , orders.fk_customer_id
            , orders.fk_territory_id
            , orders.fk_bill_address_id
            , orders.fk_ship_address_id
            , orders.fk_ship_method_id
            , orders.fk_credit_card_id
            , orders.fk_currency_rate_id
            , orders.order_date
            , orders.due_date
            , orders.ship_date
            , orders.status
            , orders.is_online_order
            , orders.purchase_order
            , orders.account_number
            , orders.sub_total
            , orders.tax_amt
            , orders.freight
            , orders.total_due
            , orders_details.fk_special_offer
            , orders_details.unit_price_discount 
            , orders_details.unit_price
            , orders_details.order_qty
            , orders.modified_date_sales_order_header
            , orders.date_key_sales_order_header
            , orders_details.modified_date_sales_order_detail
            , orders_details.date_key_sales_order_detail

        from orders_details
        left join orders on orders_details.pk_fk_salesorder_id = orders.pk_salesorder_id
        
    )

    , primary_key_created as (
        select
            cast(pk_fk_salesorder_id as varchar) || '-' || cast(fk_product_id as varchar) as sk_sales
            , *
        from joined
    )

select *
from primary_key_created