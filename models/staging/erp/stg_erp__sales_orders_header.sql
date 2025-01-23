with
    source_sales_order_header as (
        select
            cast(SALESORDERID as int) as pk_salesorder_id
            , cast(SHIPMETHODID as int) as fk_shipmethod_id
            , cast(REVISIONNUMBER as int) as revision_number
            , cast(ORDERDATE as date) as order_date
            , cast(DUEDATE as date) as due_date
            , cast(SHIPDATE as date) as ship_date
            , cast(STATUS as int) as status --status tem so o numero 5
            , cast(ONLINEORDERFLAG as boolean) as is_online_order
            , coalesce(nullif(trim(cast(PURCHASEORDERNUMBER as string)), ''), 'N/A') as purchase_order
            , coalesce(nullif(trim(cast(ACCOUNTNUMBER as string)), ''), 'N/A') as account_number
            , cast(CUSTOMERID as int) as fk_customer_id
            , cast(SALESPERSONID as int) as fk_sales_person_id
            , cast(TERRITORYID as int) as fk_territory_id
            , cast(BILLTOADDRESSID as int) as fk_bill_address_id
            , cast(SHIPTOADDRESSID as int) as fk_ship_address_id
            , cast(SHIPMETHODID as int) as fk_ship_method_id
            , cast(CREDITCARDID as int) as fk_credit_card_id
            , cast(CURRENCYRATEID as int) as fk_currency_rate_id
            , cast(SUBTOTAL as numeric(18,4)) as sub_total
            , cast(TAXAMT as numeric(18,4)) as tax_amt
            , cast(FREIGHT as numeric(18,4)) as freight
            , cast(TOTALDUE as numeric(18,4)) as total_due
            , cast(MODIFIEDDATE as date) as modified_date_sales_order_header
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_sales_order_header
        from {{source("erp_sales","salesorderheader")}}
    )
select *
from source_sales_order_header