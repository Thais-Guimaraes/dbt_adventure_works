with
    source_personcreditcard as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_business_entity_person_credit_card
            , cast(CREDITCARDID as int) as pk_fk_credit_card_id
            , cast(MODIFIEDDATE as date) as modified_date_person_credit_card
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_person_credit_card
        from {{source("erp_sales","personcreditcard")}}
    )
select *
from source_personcreditcard