with
    currency_rates as (
        select *
        from {{ ref('stg_erp__currency_rates') }}
    )
    
    , currencies as (
        select *
        from {{ ref('stg_erp__currencies') }}
    )

    , joined as (
        select
            currency_rates.pk_currency_rate_id
            , from_currency_code
            , to_currency_code
            , currency_rates.average_rate
            , currency_rates.end_day_rate
            , currencies.modified_date_currency
            , currencies.date_key_currency
            , currency_rates.modified_date_currency_rate
            , currency_rates.date_key_currency_rate
        from currency_rates 
        left join currencies as currencies on currency_rates.from_currency_code = currencies.pk_currency
        --left join currencies as currencies2 on currency_rates.to_currency_code = currencies2.pk_currency
    )

select * 
from joined