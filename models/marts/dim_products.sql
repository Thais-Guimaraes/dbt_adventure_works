with
    products as (
        select *
        from {{ ref("stg_erp__products") }}
    )
    , categories as (
        select * 
        from {{ ref("stg_erp__products_categories") }}
    )
    , sub_categories as (
        select * 
        from {{ ref("stg_erp__products_sub_categories") }}
    )
    , vendors as (
        select *
        from {{ ref("stg_erp__vendors") }}
    )
    , products_vendors as (
        select * 
        from {{ ref("stg_erp__products_vendors") }}
    )

    , bill_materials as (
        select * 
        from {{ref("stg_erp__bill_materials")}}
    )
    , joined as (
        select
            products.pk_product_id
            , products.nm_product
            , products.code_product
            , products.make_flag
            , products.finished_goods_flag
            , products.safety_stock_level
            , products.reorder_point
            , products.standard_cost
            , products.list_price
            , products.weight
            , products.days_manufacture
            , products.product_line
            , products.class
            , products.style
            , products.sell_start_date
            , products.sell_end_date
            , products.discontinued_date
            , categories.nm_category
            , sub_categories.nm_sub_category
            , vendors.nm_vendor
            , bill_materials.product_assembly_id
            , bill_materials.component_id
            , bill_materials.start_date
            , bill_materials.end_date
            , products.modified_date_product
            , products.date_key_product
            , categories.modified_date_category
            , categories.date_key_category
            , sub_categories.modified_date_sub_category
            , sub_categories.date_key_sub_category
            , vendors.modified_date_vendor
            , vendors.date_key_vendor
            , ROW_NUMBER() OVER (PARTITION BY products.pk_product_id ORDER BY vendors.pk_fk_business_entity_vendors_id) as vendor_num
        from products
        left join sub_categories on products.fk_sub_category = sub_categories.pk_sub_category
        left join categories on sub_categories.fk_category = categories.pk_category
        left join products_vendors on products.pk_product_id = products_vendors.pk_fk_product_id
        left join vendors on products_vendors.pk_fk_business_entity_product_vendors_id = vendors.pk_fk_business_entity_vendors_id
        left join bill_materials on products.pk_product_id = bill_materials.product_assembly_id
    )
select
    pk_product_id
    , nm_product
    , code_product
    , make_flag
    , finished_goods_flag
    , safety_stock_level
    , reorder_point
    , standard_cost
    , list_price
    , weight
    , days_manufacture
    , product_line
    , class
    , style
    , sell_start_date
    , sell_end_date
    , discontinued_date
    , nm_category
    , nm_sub_category
    , COALESCE(max(case when vendor_num = 1 then nm_vendor end), 'N/A') as vendor1
    , COALESCE(max(case when vendor_num = 1 then modified_date_vendor end), '1900-01-01') as modified_date_vendor1
    , COALESCE(max(case when vendor_num = 1 then date_key_vendor end), '19000101') as date_key_vendor1
    , COALESCE(max(case when vendor_num = 2 then nm_vendor end), 'N/A') as vendor2
    , COALESCE(max(case when vendor_num = 2 then modified_date_vendor end), '1900-01-01') as modified_date_vendor2
    , COALESCE(max(case when vendor_num = 2 then date_key_vendor end), '19000101') as date_key_vendor2
    , COALESCE(max(case when vendor_num = 3 then nm_vendor end), 'N/A') as vendor3
    , COALESCE(max(case when vendor_num = 3 then modified_date_vendor end), '1900-01-01') as modified_date_vendor3
    , COALESCE(max(case when vendor_num = 3 then date_key_vendor end), '19000101') as date_key_vendor3
    , modified_date_product
    , date_key_product
    , modified_date_category
    , date_key_category
    , modified_date_sub_category
    date_key_sub_category
from joined
group by
    pk_product_id
    , nm_product
    , code_product
    , make_flag
    , finished_goods_flag
    , safety_stock_level
    , reorder_point
    , standard_cost
    , list_price
    , weight
    , days_manufacture
    , product_line
    , class
    , style
    , sell_start_date
    , sell_end_date
    , discontinued_date
    , nm_category
    , nm_sub_category
    , modified_date_product
    , date_key_product
    , modified_date_category
    , date_key_category
    , modified_date_sub_category
    , date_key_sub_category