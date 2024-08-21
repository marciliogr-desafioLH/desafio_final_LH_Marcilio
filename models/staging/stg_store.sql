with
    raw_data as (
        select
            cast(businessentityid as number(38,0)) as pk_business_entity_id
            , cast(name as varchar) as name
            , cast(salespersonid as number(38,0)) as fk_salesperson_id
            , cast(demographics as varchar) as demographics
            --unused columns
            --rowguid
            --modifieddate
        from {{ source('erp', 'store') }}
    )
select *
from raw_data