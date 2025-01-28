with 
    purchase_details as (
        select
            facts.invoice_number  
            , facts.fk_credit_card_id
            , coalesce(cc.card_type, 'N/A') as card_type -- substitui null por "na"
        from {{ ref('fact_sales') }} as facts
        left join {{ ref('dim_credit_cards') }} as cc
        on facts.fk_credit_card_id = cc.pk_credit_card_id
    )
select 
    card_type
    , count(distinct invoice_number) as number_order 
from purchase_details
group by card_type
order by number_order desc
