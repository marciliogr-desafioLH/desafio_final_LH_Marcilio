with
    raw_data as (
        select
           cast(addressid as int) as pk_address_id
           , cast(addressline1 as varchar) as address_line
           , cast(city as varchar) as city
           , cast(stateprovinceid as varchar) as fk_state_province_id
           , cast(postalcode as varchar) as postal_code
           --unused columns:
           --spatiallocation
           --rowguid
           --modifieddate
           --addressline2
        from {{ source('erp', 'address') }}
    )
select *
from raw_data