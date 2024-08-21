with
    raw_data as (
        select
            cast(emailaddressid as int) as pk_email_address_id
            , cast(businessentityid as number) as fk_business_entity_id
            , cast(emailaddress as varchar) as email_address
            --unused columns:
            --rowguid-
            --modifieddate
        from {{ source('erp', 'emailaddress') }}
    )
select *
from raw_data