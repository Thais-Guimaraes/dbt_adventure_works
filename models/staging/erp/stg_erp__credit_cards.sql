with
    source_creditcards as (
        select
            cast(CREDITCARDID as int) as pk_credit_card_id
            , coalesce(nullif(trim(cast(CARDTYPE as string)), ''), 'N/A') as card_type
            , cast(MODIFIEDDATE as date) as modified_date_credit_card
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_credit_card
        from {{source("erp_sales","creditcard")}}
    )
select *
from source_creditcards