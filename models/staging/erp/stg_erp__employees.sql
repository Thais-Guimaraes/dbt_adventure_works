with
    source_employees as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_business_entity_employee_id
            , coalesce(nullif(trim(cast(JOBTITLE as string)), ''), 'N/A') as job_title
            , cast(HIREDATE as date) as hire_date
            , cast(MODIFIEDDATE as date) as modified_date_employee
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_employee
        from {{source("erp_humanresources","employee")}}
    )
select *
from source_employees