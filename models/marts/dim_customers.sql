with
    customers as (
        select *
        from {{ ref('stg_erp__customers') }}
    ),
    people as (
        select *
        from {{ ref('stg_erp__people') }}
    ),
    business_entity_addresses as (
        select *
        from {{ ref('stg_erp__business_entity_addresses') }}
    ),
    addresses as (
        select *
        from {{ ref('stg_erp__addresses') }}
    ),

    stores as (
        select 
            * 
        from {{ ref('stg_erp__stores') }} -- Refira-se à tabela que contém as lojas
    ),    
    -- Seleciona o endereço mais recente para cada cliente
    latest_addresses as (
        select
            business_entity_id,
            max(address_id) as latest_address_id
        from business_entity_addresses
        group by business_entity_id
    ),
    joined as (
        select
            people.pk_fk_business_entity_people_id as fk_person_id_customer
            , customers.pk_customer_id
            , customers.fk_territory_id
            , customers.fk_store_id
            , stores.nm_store
            --latest_addresses.latest_address_id
            , people.nm_person as nm_customer
            , people.email_promotion
            , people.total_purchase_YTD
            , people.date_first_purchase
            , people.date_birth
            , people.marital_status
            , people.yearly_income
            , people.gender
            , people.total_children
            , people.number_children_home
            , people.education
            , people.occupation
            , people.home_owner_flag
            , people.number_cars_owned
            , people.commute_distance
            , customers.modified_date_customer
            , customers.date_key_customer
            , people.modified_date_person
            , people.date_key_person
        from people
        join customers on people.pk_fk_business_entity_people_id = customers.fk_person_id_customer
        left join latest_addresses on latest_addresses.business_entity_id = customers.pk_customer_id
        left join addresses on addresses.pk_address_id = latest_addresses.latest_address_id
        left join stores on customers.fk_store_id = stores.pk_fk_businessentityid
    )
select *
from joined