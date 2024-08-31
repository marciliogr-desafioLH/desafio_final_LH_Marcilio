with stg_customer as (
    select *
    from {{ref('stg_customer')}}
)

, stg_person as (
    select *
    from {{ref('stg_person')}}
)

, stg_store as (
    select * 
    from {{ref('stg_store')}}
)

, stg_sales_territory as (
    select *
    from {{ref('stg_sales_territory')}}
)

, complete_table as (
    select
        {{ dbt_utils.generate_surrogate_key(
                    ['pk_customer_id']
                )
            }} as customer_sk -- genarate surrogate key
        , customer.pk_customer_id
        , person.pk_person_business_entity_id
        , store.pk_business_entity_id
        , territory.pk_territory_id
        , person.person_fullname
        , store.name as store_name
        , territory.name as territory_name
    from stg_customer as customer
    left join stg_person as person on customer.fk_person_id = person.pk_person_business_entity_id
    left join stg_store as store on customer.fk_store_id = store.pk_business_entity_id
    left join stg_sales_territory as territory on customer.fk_territory_id = territory.pk_territory_id
)

select *
from complete_table
