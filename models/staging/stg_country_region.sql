with
    raw_data as (
        select
           cast(countryregioncode as varchar) as pk_country_region_code
           , cast(name as varchar) as name
           --unused columns:
           --modifieddate   
        from {{ source('erp', 'countryregion') }}
    )
select *
from raw_data