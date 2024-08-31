with stg_sales_order_header as (
    select
        ship_to_address_id
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

stg_sales_territory as (
    select
        pk_territory_id,
        name,
        country_region_code
    from {{ref('stg_sales_territory')}}
),

complete_table as (
    select
        {{ dbt_utils.generate_surrogate_key(
                    ['ship_to_address_id']
                )
            }} as ship_sk -- genarate surrogate key
        , soh.ship_to_address_id
        , st.name as territory_name
        , sp.state_province_code
        , a.city
        , a.postal_code
    from stg_sales_order_header as soh
    left join stg_address as a on soh.ship_to_address_id = a.pk_address_id
    left join stg_state_province as sp on a.fk_state_province_id = sp.pk_state_province_id
    left join stg_sales_territory as st on sp.fk_territory_id = st.pk_territory_id
)

select * 
from complete_table