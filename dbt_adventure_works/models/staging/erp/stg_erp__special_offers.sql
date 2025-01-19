with
    source_specialoffer as (
        select
            cast(SPECIALOFFERID as int) as pk_special_offer
            , coalesce(nullif(trim(cast(DESCRIPTION as string)), ''), 'N/A') as desc_offer
            , cast(DISCOUNTPCT as numeric(18,2)) as discount_pct
            , coalesce(nullif(trim(cast(TYPE as string)), ''), 'N/A') as type_offer
            , coalesce(nullif(trim(cast(CATEGORY as string)), ''), 'N/A') as category_offer
            , cast(STARTDATE as date) as start_date
            , cast(ENDDATE as date) as enddate
            , cast(MINQTY as int) as min_qty
            , cast(MAXQTY as int) as max_qty
            , cast(MODIFIEDDATE as date) as modified_date_special_offer
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_special_offer
        from {{source("erp_sales","specialoffer")}}
    )
select *
from source_specialoffer