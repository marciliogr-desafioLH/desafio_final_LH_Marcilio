with stg_sales_order_header as (
    select
       distinct(ship_to_address_id)
    from {{ref('stg_sales_order_header')}}
),

stg_address as (
    select *
    from {{ref('stg_address')}}
),

stg_state_province as (
    select *
    from {{ref('stg_state_province')}}
),

stg_country_region as (
    select *
    from {{ref('stg_country_region')}}
),

complete_table as (
    select
        {{ dbt_utils.generate_surrogate_key(
                    ['ship_to_address_id']
                )
            }} as ship_sk -- genarate surrogate key
        , soh.ship_to_address_id
        , cr.name as country
        , sp.name as state
        , a.city
        --, a.postal_code
    from stg_sales_order_header as soh
    left join stg_address as a on soh.ship_to_address_id = a.pk_address_id
    left join stg_state_province as sp on a.fk_state_province_id = sp.pk_state_province_id
    left join stg_country_region as cr on sp.fk_country_region_code = cr.pk_country_region_code
)

select * 
from complete_table