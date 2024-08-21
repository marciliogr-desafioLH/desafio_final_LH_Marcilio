with
    raw_data as (
        select
           	cast(territoryid as int) as pk_territory_id
            , cast(name as varchar) as name
            , cast(countryregioncode as varchar) as country_region_code
            , cast(salesytd as float) as sales_ytd
            , cast(saleslastyear as float) as last_year_sales
            , cast(costytd as int) as cost_ytd
            , cast(costlastyear as int) as last_year_cost
            --unused columns:
            --groups
            --rowguid 
            --modifieddate 
        from {{ source('erp', 'salesterritory') }}
    )
select *
from raw_data