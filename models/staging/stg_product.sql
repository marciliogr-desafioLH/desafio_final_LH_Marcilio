with
    raw_data as (
        select
            cast(productid as int) as pk_product_id
            , cast(name as varchar) as name
            , cast(productnumber as varchar) as product_number
            , cast(color as varchar) as color
            , cast(safetystocklevel as int) as safety_stock_level
            , cast(reorderpoint as int) as reorder_point
            , cast(standardcost as float) as standard_cost
            , cast(listprice as float) as list_price
            , cast(size as varchar) as size
            , cast(sizeunitmeasurecode as varchar) as size_unit_measure_code
            , cast(weightunitmeasurecode as varchar) as weight_unit_measure_code
            , cast(weight as float) as weight
            , cast(daystomanufacture as int) as days_to_manufacture
            , cast(productline as varchar) as product_line
            , cast(class as varchar) as class
            , cast(style as varchar) as style
            , cast(productsubcategoryid as int) as product_subcategory_id
            , cast(productmodelid as int) as product_model_id
            , sellstartdate::date as sell_start_date
            --unused columns:
            --makeflag
            --finishedgoodsflag
            --rowguid
            --modifieddate
            --sellenddate
            --discontinueddate
        from {{ source('erp', 'product') }}
    )
select *
from raw_data