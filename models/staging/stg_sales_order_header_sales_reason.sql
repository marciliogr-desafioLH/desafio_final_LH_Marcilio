with
    raw_data as (
        select       
            cast(salesorderid as int) as fk_sales_order_id
            , cast(salesreasonid as int) as fk_sales_reason_id
            --unused columns:
            --modifieddate
        from {{ source('erp', 'salesorderheadersalesreason') }}
    )
select *
from raw_data