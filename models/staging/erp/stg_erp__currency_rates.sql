with
    source_currencyrates as (
        select
            cast(CURRENCYRATEID as int) as pk_currency_rate_id
            , cast(CURRENCYRATEDATE  as date) as date_currency_rate
            , cast(FROMCURRENCYCODE  as string) as from_currency_code
            , cast(TOCURRENCYCODE  as string) as to_currency_code
            , cast(AVERAGERATE  as numeric(18,4)) as average_rate
            , cast(ENDOFDAYRATE  as numeric(18,4)) as end_day_rate
            , cast(MODIFIEDDATE as date) as modified_date_currency_rate
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_currency_rate
        from {{source("erp_sales","currencyrate")}}
    )
select *
from source_currencyrates