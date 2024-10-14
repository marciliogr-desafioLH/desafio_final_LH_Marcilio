with
    -- Load dimension tables
    cliente as (
        select 
            *
        from {{ ref("dim_customers") }}
    ),

    cartao_credito as (
        select
            *
        from {{ref('dim_creditcard')}}
    ),

    localizacao as (
        select
            *
        from {{ref('dim_locations')}}
    ),

    produtos as (
        select
            *
        from {{ref('dim_products')}}
    ),

    motivo_vendas as (
        select 
            *
        from {{ ref("dim_sales_reason") }}
    ),

    vendedor as (
        select 
            *
        from {{ref('dim_employee')}} 
    ),

    -- Load staging tables
    salesorderdetail as (
        select 
            *
        from {{ref('stg_sales_order_detail')}}
    ),

    salesorderheader as (
        select 
            *
        from {{ref('stg_sales_order_header')}}
    ),

    intermediate as (
        select 
            * 
        from {{ ref("int_orders_details") }}
    ),

    -- Join tables and calculate metrics
    final as (
        select
            {{ dbt_utils.generate_surrogate_key(
                        ['sales_sk','pk_order_detail_id']
                    )
                }} as sales_fact_sk
            , int_orders_details.sales_sk as fk_sales
            , int_orders_details.pk_order_detail_id as fk_order_detail_id
            , int_orders_details.fk_product
            , int_orders_details.fk_customer
            , int_orders_details.fk_order
            , int_orders_details.fk_salesperson
            , int_orders_details.fk_territory
            , int_orders_details.fk_shipping_address
            , int_orders_details.fk_credit_card
            , int_orders_details.order_date
            , int_orders_details.ship_date
            , int_orders_details.purchase_order_number
            , int_orders_details.order_quantity
            , int_orders_details.unit_price
            , int_orders_details.unit_price_discount
            , int_orders_details.net_total
            , int_orders_details.allocated_freight
            , int_orders_details.allocated_tax
            , int_orders_details.gross_total
            , dim_customers.person_fullname as customer
            , dim_customers.store_name as store
            , dim_sales_reason.reason_name_aggregated
        from int_orders_details
        left join dim_customers on int_orders_details.fk_customer = dim_customers.pk_customer_id
        left join dim_sales_reason on int_orders_details.fk_order = dim_sales_reason.fk_sales_order_id
    ),

    -- Organize final columns for output
    organizar_colunas as (
        select
            sales_fact_sk
            , fk_sales
            , fk_order
            , fk_order_detail_id
            , fk_product
            , fk_customer
            , fk_credit_card
            , fk_salesperson
            , fk_shipping_address
            , fk_territory
            , order_date
            , ship_date            
            , customer
            --, tipo_pessoa
            , purchase_order_number
            --, pedido_online
            , order_quantity
            , reason_name_aggregated
            , unit_price
            , unit_price_discount
            , net_total
            , allocated_freight
            , allocated_tax
            , gross_total
            --, nome_status_pedido
            , store
        from final
    )

-- Final fact table output
select *
from organizar_colunas
