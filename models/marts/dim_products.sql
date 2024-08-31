with stg_salesorderheader as (
    select *
    from {{ref('stg_sales_order_header')}}
)

, stg_salesorderdetail as (
    select 
        distinct(product_id)
    from {{ref('stg_sales_order_detail')}}
)

, stg_product as (
    select *
    from {{ref('stg_product')}}
)

, complete_table as (
    select
        {{ dbt_utils.generate_surrogate_key(
                    ['product_id']
                )
            }} as product_sk -- genarate surrogate key
        , stg_sales_order_detail.product_id
        , stg_product.name 
    from stg_sales_order_detail
    left join stg_product on stg_sales_order_detail.product_id = stg_product.pk_product_id
)

select *
from complete_table