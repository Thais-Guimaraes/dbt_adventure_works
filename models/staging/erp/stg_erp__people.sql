with
    source_people as (
        select
            cast(BUSINESSENTITYID as int) as pk_fk_business_entity_people_id
            , coalesce(nullif(trim(PERSONTYPE), ''), 'N/A') as person_type
            , coalesce(nullif(trim(FIRSTNAME), ''), 'N/A') || ' ' || coalesce(nullif(trim(LASTNAME), ''), 'N/A') as nm_person
            , cast(EMAILPROMOTION as boolean) as email_promotion
            , cast(get(XMLGET(DEMOGRAPHICS, 'TotalPurchaseYTD'), '$') as numeric(18,4)) as total_purchase_YTD
            , cast(left(xmlget(DEMOGRAPHICS, 'DateFirstPurchase'):"$", 10) as date) as date_first_purchase
            , cast(left(xmlget(DEMOGRAPHICS, 'BirthDate'):"$", 10) as date) as date_birth
            , coalesce(nullif(trim(cast(xmlget(DEMOGRAPHICS, 'MaritalStatus'):"$" as string)), ''), 'N/A') as marital_status
            , coalesce(nullif(trim(cast(xmlget(DEMOGRAPHICS, 'YearlyIncome'):"$" as string)), ''), 'N/A') as yearly_income
            , coalesce(nullif(trim(cast(xmlget(DEMOGRAPHICS, 'Gender'):"$" as string)), ''), 'N/A') as gender
            , coalesce(nullif(trim(cast(xmlget(DEMOGRAPHICS, 'TotalChildren'):"$" as int)), ''), 'N/A') as total_children
            , cast(xmlget(DEMOGRAPHICS, 'NumberChildrenAtHome'):"$" as int) as number_children_home
            , coalesce(nullif(trim(cast(xmlget(DEMOGRAPHICS, 'Education'):"$" as string)), ''), 'N/A') as education
            , coalesce(nullif(trim(cast(xmlget(DEMOGRAPHICS, 'Occupation'):"$" as string)), ''), 'N/A') as occupation
            , coalesce(nullif(trim(cast(xmlget(DEMOGRAPHICS, 'HomeOwnerFlag'):"$" as string)), ''), 'N/A') as home_owner_flag
            , cast(xmlget(DEMOGRAPHICS, 'NumberCarsOwned'):"$" as int) as number_cars_owned
            , coalesce(nullif(trim(cast(xmlget(DEMOGRAPHICS, 'CommuteDistance'):"$" as string)), ''), 'N/A') as commute_distance
            , cast(MODIFIEDDATE as date) as modified_date_person
            , to_number(to_char(cast(MODIFIEDDATE as date), 'YYYYMMDD')) as date_key_person
        from {{ source("erp_person", "person") }}
    )
select *
from source_people