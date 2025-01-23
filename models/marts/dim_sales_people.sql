with
    sales_people as (
        select *
        from {{ ref('stg_erp__sales_people') }}
    )
    
    
    , sales_people_quotas as (
        select *
        from {{ ref('stg_erp__sales_people_quotas') }}
    )

    , employees as (
        select *
        from {{ ref('stg_erp__employees') }}
    )

    , people as (
        select *
        from {{ ref('stg_erp__people') }}
    )


    , joined as (
        select
            sales_people_quotas.pk_fk_business_entity_people_quotas_id
            , sales_people.pk_fk_business_entity_sales_people_id 
            , employees.pk_fk_business_entity_employee_id
            , people.pk_fk_business_entity_people_id
            , people.nm_person as nm_sales_people
            , sales_people.modified_date_sales_person
            , sales_people.date_key_sales_person
            --, sales_people_quotas.modified_date_person_quota
            --, sales_people_quotas.date_key_person_quota
            , employees.job_title
            , employees.hire_date
            , employees.modified_date_employee
            , employees.date_key_employee
            , people.modified_date_person
            , people.date_key_person
        from sales_people
        left join sales_people_quotas on sales_people.pk_fk_business_entity_sales_people_id = sales_people_quotas.pk_fk_business_entity_people_quotas_id
        left join employees on sales_people.pk_fk_business_entity_sales_people_id  = employees.pk_fk_business_entity_employee_id
        left join people on employees.pk_fk_business_entity_employee_id = people.pk_fk_business_entity_people_id
             
    )

select DISTINCT * 
from joined