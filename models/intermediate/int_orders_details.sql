with 
    salesorderheader as (
        select *
            , pk_sales_order_id as fk_sales_order_id
        from {{ref('stg_sales_order_header')}}
    )

    , orderdetail as (
        select *
        from {{ref('stg_sales_order_detail')}}
    )

    , joined as (
        select
            orderdetail.pk_sales_order_detail_id
            , orderdetail.product_id as fk_product
            , salesorderheader.fk_customer_id as fk_customer
            , salesorderheader.fk_sales_order_id as fk_order
            , salesorderheader.fk_sales_person_id as fk_salesperson
            , salesorderheader.fk_territory_id as fk_territory
            , salesorderheader.ship_to_address_id as fk_shipping_address
            , salesorderheader.fk_creditcard_id as fk_credit_card
            , salesorderheader.order_date as order_date
            , salesorderheader.shipdate as ship_date
            , salesorderheader.purchase_order_number as purchase_order_number
            , orderdetail.order_qty as order_quantity
            , orderdetail.unit_price as unit_price
            , orderdetail.unit_price_discount as unit_price_discount
            , salesorderheader.freight as freight
            , salesorderheader.taxa_mt as tax_amount                
        from orderdetail
        inner join salesorderheader on orderdetail.fk_sales_order_id = salesorderheader.fk_sales_order_id
    )

    , metrics as (
        select
            *
            -- value related to the product price without tax and freight.
            , unit_price * (1 - unit_price_discount) * order_quantity as net_total
            -- freight allocated by the quantity of items in the same order.
            , cast(freight / count(*) over(partition by fk_order) as numeric(18,2)) as allocated_freight
            -- tax allocated by the quantity of items in the same order.
            , cast(tax_amount / count(*) over(partition by fk_order) as numeric(18,2)) as allocated_tax
            , unit_price * order_quantity * (1 - unit_price_discount) + allocated_tax + allocated_freight as gross_total
        from joined
    )
    -- created a surrogate key using fk_order and fk_product.
    , surrogate_key as (
        select
            fk_order::varchar || '-' || fk_product::varchar as sales_sk
            , *
        from metrics
    )

    , organize_columns as (
        select
            sales_sk
            , pk_sales_order_detail_id as pk_order_detail_id
            , fk_product
            , fk_customer
            , fk_order
            , fk_salesperson
            , fk_territory
            , fk_shipping_address
            , fk_credit_card
            , order_date
            , ship_date
            , purchase_order_number
            , order_quantity
            , unit_price
            , unit_price_discount
            , net_total
            , allocated_freight
            , allocated_tax
            , gross_total
        from surrogate_key
    )
select *
from organize_columns
