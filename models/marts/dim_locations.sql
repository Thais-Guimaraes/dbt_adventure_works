with
    country_regions as (
        select *
        from {{ ref('stg_erp__country_regions') }}
    ),
    state_provinces as (
        select *
        from {{ ref('stg_erp__state_provinces') }}
    ),
    addresses as (
        select *
        from {{ ref('stg_erp__addresses') }}
    ),
    salesterritory as (
        select *
        from {{ ref('stg_erp__sales_territories') }}
    ),
    joined as (
        select
            addresses.pk_address_id
            , country_regions.pk_country_region
            , state_provinces.pk_state_province_id
            , salesterritory.pk_territory_id
            , country_regions.nm_country
            , state_provinces.code_province
            , state_provinces.is_only_state
            , state_provinces.nm_state_province
            , addresses.city
            , country_regions.modified_date_country_region
            , country_regions.date_key_country_region
            , state_provinces.modified_date_state_province
            , state_provinces.date_key_state_province
            , addresses.modified_date_address
            , addresses.date_key_address
            , salesterritory.modified_date_territory
            , salesterritory.date_key_territory
            --, ROW_NUMBER() OVER (
                --PARTITION BY 
                   -- addresses.city 
                   -- , state_provinces.code_province 
                   -- , state_provinces.nm_state_province 
                   -- , country_regions.nm_country 
               -- ORDER BY 
                  --  addresses.modified_date_address DESC
           -- ) as row_num
        from addresses
        left join state_provinces on addresses.fk_state_province_id = state_provinces.pk_state_province_id
        left join country_regions on state_provinces.fk_country_region_code = country_regions.pk_country_region
        left join salesterritory on state_provinces.fk_territory_id = salesterritory.pk_territory_id
    )
select
    pk_address_id
    , pk_country_region
    , pk_state_province_id
    , pk_territory_id
    , nm_country
    , code_province
    , is_only_state
    , nm_state_province
    , city
    , modified_date_country_region
    , date_key_country_region
    , modified_date_state_province
    , date_key_state_province
    , modified_date_address
    , date_key_address
    , modified_date_territory
    , date_key_territory
from joined
--where row_num = 1