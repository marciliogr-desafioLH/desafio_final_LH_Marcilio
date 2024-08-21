with
    raw_data as (
        select
            cast(customerid as int) as pk_customer_id
            , cast(personid as int) as fk_person_id -- 4% em branco 
            , cast(storeid as int) as fk_store_id
            , cast(territoryid as int) fk_territory_id
            --unused columns:
            --rowguid 
            --modifieddate
        from {{ source('erp', 'customer') }}
    )
select *
from raw_data