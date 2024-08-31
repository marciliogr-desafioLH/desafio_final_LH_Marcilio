with stg_sales_order_header_sales_reason as (
    select *
    from {{ref('stg_sales_order_header_sales_reason')}}
)

, stg_sales_reason as (
    select *
    from {{ref('stg_sales_reason')}}
)

, reasonbyorderid as (
    select 
        stg_sales_order_header_sales_reason.fk_sales_order_id
        , stg_sales_reason.sales_reason_name as reason_name
    from stg_sales_order_header_sales_reason
    left join stg_sales_reason on stg_sales_order_header_sales_reason.fk_sales_reason_id = stg_sales_reason.pk_sales_reason_id 
)

, complete_table as (
    select
        {{ dbt_utils.generate_surrogate_key(
                    ['fk_sales_order_id']
                )
            }} as sales_order_sk -- genarate surrogate key
        , fk_sales_order_id -- aggregate function 
        , listagg(reason_name, ', ') as reason_name_aggregated
    from reasonbyorderid
    group by fk_sales_order_id
)

select *
from complete_table