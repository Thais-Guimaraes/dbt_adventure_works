-- recency, frequency, monetary (rfm) analysis
with 
    max_date as (
    -- get the latest order date from the fact table (considering it as the reference point)
        select
            dateadd(day, 1, max(order_date)) as lastdate
        from {{ ref('fact_sales') }}
    )
    , recency as (
    -- get the last purchase date for each customer
        select
            facts.fk_customer_id as customerid
            , max(facts.order_date) as lastpurchasedate
        from {{ ref('fact_sales') }} as facts
        group by facts.fk_customer_id
    )
    , frequency as (
    -- count the number of purchases for each customer
        select
            facts.fk_customer_id as customerid
            , count(distinct(facts.invoice_number)) as purchasecount
        from {{ ref('fact_sales') }} as facts
        group by facts.fk_customer_id
    )
    , monetary as (
        -- calculate the total amount spent by each customer
        select
            facts.fk_customer_id as customerid
            , sum(facts.gross_value) as totalspent
        from {{ ref('fact_sales') }} as facts
        group by facts.fk_customer_id
)
-- combine recency, frequency, and monetary metrics
select
    r.customerid
    , datediff(day, r.lastpurchasedate, md.lastdate) as recencydays -- days since last purchase
    , f.purchasecount as frequency -- number of purchases
    , m.totalspent as monetaryvalue -- total amount spent
from recency r
join frequency f
    on r.customerid = f.customerid
join monetary m
    on r.customerid = m.customerid
cross join max_date md -- use the calculated lastdate as the reference point
order by recencydays asc, frequency desc, monetaryvalue desc -- prioritize based on rfm metrics
