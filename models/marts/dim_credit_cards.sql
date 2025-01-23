with
    credit_cards as (
        select *
        from {{ ref('stg_erp__credit_cards') }}
    )

    , people_credit_cards as (
        select *
        from {{ ref('stg_erp__people_credit_cards') }}
    )

    , people as (
        select *
        from {{ ref('stg_erp__people') }}
    )

    , joined as (
        select
            cc.pk_credit_card_id 
            , cc.card_type
            , p.nm_person
            , cc.modified_date_credit_card
            , cc.date_key_credit_card
            , pcc.modified_date_person_credit_card
            , pcc.date_key_person_credit_card
            , p.modified_date_person
            , p.date_key_person
        from people_credit_cards as pcc
        left join credit_cards as cc on pcc.pk_fk_credit_card_id = cc.pk_credit_card_id
        left join people as p on pcc.pk_fk_business_entity_person_credit_card = p.pk_fk_business_entity_people_id
    )

select * 
from joined