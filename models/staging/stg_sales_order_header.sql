with
    raw_data as (
        select
            cast(salesorderid as int) as pk_sales_order_id
            , cast(orderdate as date) as order_date
            , shipdate::date as shipdate
            , cast(status as varchar) as salesorder_status            
            , cast(purchaseordernumber as varchar) as purchase_order_number
            , cast(customerid as varchar) as fk_customer_id
            , cast(salespersonid as varchar) as fk_sales_person_id 
            , cast(territoryid as varchar) as fk_territory_id
            , cast(shiptoaddressid as varchar) as ship_to_address_id
            , cast(shipmethodid as varchar) as ship_method_id
            , cast(creditcardid as varchar) as fk_creditcard_id
            , cast(subtotal as float) as sales_subtotal
            , cast(taxamt as float) as taxa_mt
            , cast(freight as float) as freight
            , cast(totaldue as float) as total_due
            --unused columns:
            --revisionnumber 
            --duedate
            --onlineorderflag
            --accountnumber
            --billtoaddressid
            --creditcardapprovalcode
            --currencyrateid
            --rowguid
            --modifieddate 
        from {{ source("erp", "salesorderheader") }}
    )
select *
from
    raw_data