with 
    sales as (
        select
            facts.invoice_number as salesorderid,
            facts.total_due as totaldue
        from {{ ref('fact_sales') }} as facts
    )
    , sales_reasons as (
        select
            dsr.pk_fk_sales_order_id as salesorderid,
            dsr.concatenated_reason_names as reasonname
        from {{ ref('dim_sales_reasons') }} as dsr
    )

select
    sr.reasonname as motivocompra
    , sum(s.totaldue) as valortotalvendas
from sales s
join sales_reasons sr
    on s.salesorderid = sr.salesorderid
group by sr.reasonname
order by valortotalvendas desc
