with
    source_bill_materials as (
        select
            cast(BILLOFMATERIALSID as int) as pk_billofmaterialsid
            , cast(PRODUCTASSEMBLYID as int) as product_assembly_id
            , cast(COMPONENTID as int) as component_id
            , cast(STARTDATE as date) as start_date
            , cast(ENDDATE as date) as end_date
            , coalesce(nullif(trim(cast(UNITMEASURECODE as string)), ''), 'N/A') as unit_measure_code
            , cast(BOMLEVEL as int) as bom_level
            , cast(PERASSEMBLYQTY as numeric(18,2)) as per_assembly_qty
            , cast(MODIFIEDDATE as date) as modified_date_store
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_store
        from {{source("erp_production","billofmaterials")}}
    )
select *
from source_bill_materials