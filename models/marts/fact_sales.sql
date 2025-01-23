WITH
    weight_totals AS (
        SELECT *
        FROM {{ ref('weight_totals') }}
    )
    , orders_items AS (
        SELECT *
        FROM {{ ref('int_orders_items') }}
    )
    , dim_products AS (
        SELECT *
        FROM {{ ref('dim_products') }}
    )
    , dim_sales_people AS (
        SELECT *
        FROM {{ ref('dim_sales_people') }}
    )
    , ranked_dim_locations AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY pk_address_id ORDER BY modified_date_address DESC) AS row_num
        FROM {{ ref('dim_locations') }}
    )
    , dim_locations AS (
        SELECT *
        FROM ranked_dim_locations
        WHERE row_num = 1
    )
    , dim_credit_cards AS (
        SELECT *
        FROM {{ ref('dim_credit_cards') }}
    )
    , dim_currency_rates AS (
        SELECT *
        FROM {{ ref('dim_currency_rates') }}
    )
    , dim_customers AS (
        SELECT *
        FROM {{ ref('dim_customers') }}
    )
    , dim_sales_reasons AS (
        SELECT *
        FROM {{ ref('dim_sales_reasons') }}
    )
    , dim_sales_tax_rates AS (
        SELECT *
        FROM {{ ref('dim_sales_tax_rates') }}
    )
    , dim_dates AS (
        select *
        from {{ ref('dim_dates') }}
    )
    , joined AS (
        select
            facts.sk_sales
            , facts.pk_fk_salesorder_id as invoice_number
            , facts.fk_product_id
            , facts.fk_customer_id
            , facts.fk_sales_person_id
            , facts.fk_territory_id
            , facts.fk_bill_address_id
            , facts.fk_ship_address_id
            , facts.fk_ship_method_id
            , facts.fk_credit_card_id
            , facts.fk_currency_rate_id
            , facts.fk_special_offer
            , facts.order_date
            , facts.due_date
            , facts.ship_date
            , facts.status
            , facts.is_online_order
            , facts.purchase_order
            , facts.account_number
            , facts.sub_total
            , facts.tax_amt
            , facts.freight
            , facts.total_due
            , facts.modified_date_sales_order_header
            , facts.date_key_sales_order_header
            , facts.unit_price_discount
            , facts.unit_price
            , facts.order_qty
            /*, dim_products.nm_product
            , dim_products.code_product
            , dim_products.make_flag
            , dim_products.finished_goods_flag
            , dim_products.safety_stock_level
            , dim_products.reorder_point
            , dim_products.standard_cost
            , dim_products.list_price
            , dim_products.days_manufacture
            , dim_products.product_line
            , dim_products.class
            , dim_products.style
            , dim_products.sell_start_date
            , dim_products.sell_end_date
            , dim_products.discontinued_date
            , dim_products.nm_category
            , dim_products.nm_sub_category
            , dim_products.vendor1
            , dim_products.vendor2
            , dim_products.vendor3
            , dim_products.modified_date_product
            , dim_products.date_key_product
            , dim_products.modified_date_category
            , dim_products.date_key_category
            */, dim_products.weight /*
            , dim_sales_people.nm_sales_people
            , dim_sales_people.job_title
              */, dim_sales_people.hire_date/*
            , dim_locations.city
            , dim_locations.nm_state_province
            , dim_locations.nm_country
            , dim_credit_cards.card_type
            , dim_credit_cards.modified_date_credit_card
            , dim_credit_cards.date_key_credit_card
            , dim_credit_cards.modified_date_person_credit_card
            , dim_credit_cards.date_key_person_credit_card
            , dim_currency_rates.average_rate
            , dim_currency_rates.end_day_rate
            , dim_currency_rates.modified_date_currency
            , dim_currency_rates.date_key_currency
            , dim_currency_rates.modified_date_currency_rate
            , dim_currency_rates.date_key_currency_rate
            --, dim_customers.nm_customer
            --, dim_customers.email_promotion
           -- , dim_customers.modified_date_customer
            --, dim_customers.date_key_customer
            --, dim_customers.modified_date_person
            --, dim_customers.date_key_person
            --, dim_sales_reasons.concatenated_reason_names
            --, dim_dates."DATE"'''*/
        from orders_items as facts
        left join dim_products on facts.fk_product_id = dim_products.pk_product_id
        left join dim_sales_people on facts.fk_sales_person_id = dim_sales_people.pk_fk_business_entity_sales_people_id
        --left join dim_locations on facts.fk_bill_address_id = dim_locations.pk_address_id
        --left join dim_credit_cards on facts.fk_credit_card_id = dim_credit_cards.pk_credit_card_id
        --left join dim_currency_rates on facts.fk_currency_rate_id = dim_currency_rates.pk_currency_rate_id
        --left join dim_customers on facts.fk_customer_id = dim_customers.pk_customer_id
        --left join dim_sales_reasons on facts.pk_fk_salesorder_id = dim_sales_reasons.pk_fk_sales_order_id
        --left join dim_dates on facts.order_date = dim_dates."DATE"
    )
    , line_counts as ( --count lines from invoice
        select
            invoice_number,
            count(*) as line_count
        from joined
        group by invoice_number
    )

    , metrics as (
        select
            joined.*,
            line_counts.line_count
            , cast((hire_date - order_date) / 365 as numeric(18, 2)) as seniority_years
            , case
                -- Se algum produto tem peso nulo, distribui o frete proporcional à quantidade
            when exists (
            select 1
            from joined as j
            where j.invoice_number = joined.invoice_number
            and j.weight is null
            ) then 
                -- Distribui o frete proporcional à quantidade de cada item na fatura
            joined.freight / line_counts.line_count
            else 
            -- Se todos os produtos têm peso, distribui o frete proporcional ao peso
                joined.freight * (joined.order_qty * joined.weight) / 
                    (select sum(COALESCE(order_qty * weight, 0)) 
                     from joined as j2 
                    where j2.invoice_number = joined.invoice_number)
            end as shared_freight
            , case
                when joined.weight is not null then (joined.order_qty * joined.weight) / weight_totals.total_weight * 100.0
                else NULL
            end as percent_weight
            , order_qty * unit_price as gross_value
            , (joined.tax_amt / line_counts.line_count) as distributed_tax_amt
            , ((order_qty * unit_price) + (distributed_tax_amt) + shared_freight - (order_qty * unit_price * unit_price_discount)) as net_value
        from joined
        left join weight_totals on joined.invoice_number = weight_totals.salesorder_id
        left join line_counts on joined.invoice_number = line_counts.invoice_number
    )
select *
from metrics