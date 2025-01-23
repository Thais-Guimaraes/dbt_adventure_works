with
    sales_header_reasons as (
        select *
        from {{ ref('stg_erp__sales_header_reasons') }}
    )
    
    
    , sales_reasons as (
        select *
        from {{ ref('stg_erp__sales_reasons') }}
    )

    , joined as (
        select
            sales_header_reasons.pk_fk_sales_order_id
            , listagg(sales_reasons.pk_sales_reason_id, ',') within group (order by sales_reasons.pk_sales_reason_id) as concatenated_reason_ids
            , listagg(sales_reasons.nm_sales_reason, ', ') within group (order by sales_reasons.pk_sales_reason_id) as concatenated_reason_names
            , listagg(sales_reasons.reason_type, ', ') within group (order by sales_reasons.pk_sales_reason_id) as concatenated_reason_types
            , max(sales_header_reasons.modified_date_sales_header_reason) as modified_date_sales_header_reason
            , max(sales_header_reasons.date_key_sales_header_reason) as date_key_sales_header_reason
            , max(sales_reasons.modified_date_sales_reason) as modified_date_sales_reason
            , max(sales_reasons.date_key_sales_reason) as date_key_sales_reason
        from sales_header_reasons
            left join sales_reasons on sales_header_reasons.pk_fk_sales_reason_id = sales_reasons.pk_sales_reason_id
        group by
            sales_header_reasons.pk_fk_sales_order_id   
         
    )

select distinct * 
from joined