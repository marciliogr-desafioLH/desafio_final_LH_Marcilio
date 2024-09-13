with
    raw_data as (
        select
            cast(salesorderid as int) as fk_sales_order_id
            , cast(salesorderdetailid as int) as pk_sales_order_detail_id
            , cast(carriertrackingnumber as varchar) as carrier_tracking_number
            , cast(orderqty as int) as order_qty
            , cast(productid as int) as product_id
            , cast(specialofferid as int) as special_offer_id
            , cast(unitprice as float) as unit_price
            , cast(unitpricediscount as float) as unit_price_discount
            , (unitprice * (1 - unitpricediscount) * orderqty) subtotal
            --unused columns:
            --rowguid
            --modifieddate
        from {{ source('erp', 'salesorderdetail') }}
    )
select *
from raw_data