with
    raw_data as (
        select
            cast(stateprovinceid as int) as pk_state_province_id
            , cast(stateprovincecode as varchar) as state_province_code
            , cast(countryregioncode as varchar) as fk_country_region_code
            , cast(isonlystateprovinceflag as boolean) as is_only_state_province_flag
            , cast(name as varchar) as name
            , cast(territoryid as int) as fk_territory_id
            --unused columns:
            --rowguid
            --modifieddate
        from {{ source('erp', 'stateprovince') }}
    )  
select *
from raw_data