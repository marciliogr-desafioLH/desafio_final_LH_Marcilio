with
    raw_data as (
        select
            cast(salesreasonid as int) as pk_sales_reason_id
            , cast(name as varchar) as sales_reason_name
            , cast(reasontype as varchar) as sales_quota
            --unused columns:
            --modifieddate 
        from {{ source('erp', 'salesreason') }}
    )
select *
from raw_data