with stg_sales_order_header as (
    select 
        distinct(fk_creditcard_id)
    from {{ref('stg_sales_order_header')}}
    where fk_creditcard_id is not null
)

, stg_creditcard as (
    select *
    from {{ref('stg_creditcard')}}
)

, complete_table as (
    select
        {{ dbt_utils.generate_surrogate_key(
                    ['fk_creditcard_id','card_number']
                )
            }} as creditcard_sk -- genarate surrogate key
        , stg_sales_order_header.fk_creditcard_id
        , stg_creditcard.card_type
    from stg_sales_order_header 
    left join stg_creditcard on stg_sales_order_header.fk_creditcard_id = stg_creditcard.pk_credit_card_id
)

select *
from complete_table