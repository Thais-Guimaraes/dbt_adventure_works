-- recency, frequency, monetary (rfm) analysis
with 
    max_date as (
    
        select
            dateadd(day, 1, max(order_date)) as lastdate
        from {{ ref('fact_sales') }}
    )
    , recency as (
    
        select
            facts.fk_customer_id as customerid
            , max(facts.order_date) as lastpurchasedate
        from {{ ref('fact_sales') }} as facts
        group by facts.fk_customer_id
    )
    , frequency as (
   
        select
            facts.fk_customer_id as customerid
            , count(distinct(facts.invoice_number)) as purchasecount
        from {{ ref('fact_sales') }} as facts
        group by facts.fk_customer_id
    )
    , monetary as (
        
        select
            facts.fk_customer_id as customerid
            , sum(facts.gross_value) as totalspent
        from {{ ref('fact_sales') }} as facts
        group by facts.fk_customer_id
)

select
    r.customerid
    , datediff(day, r.lastpurchasedate, md.lastdate) as recencydays 
    , f.purchasecount as frequency 
    , m.totalspent as monetaryvalue 
from recency r
join frequency f
    on r.customerid = f.customerid
join monetary m
    on r.customerid = m.customerid
cross join max_date md 
order by recencydays asc, frequency desc, monetaryvalue desc 
