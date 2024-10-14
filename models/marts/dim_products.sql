with stg_product as (
    select *
    from {{ ref('stg_product') }}
),

complete_table as (
    select
        {{ dbt_utils.generate_surrogate_key(
                    ['pk_product_id','product_number']
                )
            }} as product_sk -- genarate surrogate key
        , stg_product.pk_product_id
        , stg_product.name
    from stg_product
)

select
    product_sk
    , pk_product_id
    , name
from complete_table