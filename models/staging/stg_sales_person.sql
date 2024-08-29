with raw_data as (
    select
        cast(businessentityid as int) as pk_sales_person_id
        , cast(territoryid as int) as territory_id
        , cast(salesquota as int) as sales_quota
        , cast(bonus as int) as bonus
        , cast(commissionpct as float) as commission_pct
        , cast(salesytd as float) as sales_ytd
        , cast(saleslastyear as float) as sales_last_year
        --unused columns:
        --rowguid
        --modifieddate
    from {{ source('erp', 'salesperson') }}
)

select *
from raw_data
