with
    source_department as (
        select
            cast(DEPARTMENTID as int) as pk_departmentid
            , coalesce(nullif(trim(cast(NAME as string)), ''), 'N/A') as nm_department
             , coalesce(nullif(trim(cast(GROUPNAME as string)), ''), 'N/A') as nm_group
            , cast(MODIFIEDDATE as date) as modified_date_department
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_department
        from {{source("erp_humanresources","department")}}
    )
select *
from source_department