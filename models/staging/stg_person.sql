with
    raw_data as (
        select
            cast(businessentityid as varchar) as pk_person_business_entity_id
            , concat(ifnull(firstname,' '),' ',ifnull(middlename,' '),' ',ifnull(lastname,' ')) as person_fullname
            , cast(persontype as varchar) as person_type
            --unused columns:
            --namestyle
            --suffix
            --emailpromotion
            --additionalcontactinfo
            --demographics
            --rowguid
            --modifieddate
        from {{ source('erp', 'person') }}
    )
select *
from raw_data