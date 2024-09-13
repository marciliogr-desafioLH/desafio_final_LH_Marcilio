with
    sales_orders as (
        select *
        from {{ ref('stg_sales_order_header') }}
    ),
    
    sales_order_details as (
        select *
        from {{ ref('stg_sales_order_detail') }}
    ),
    
    sales_territory as (
        select *
        from {{ ref('stg_sales_territory') }}
    ),
    
    state_province as (
        select *
        from {{ ref('stg_state_province') }}
    ),
    
    country_region as (
        select *
        from {{ ref('stg_country_region') }}
    ),
    
    joined as (
        select 
            sales_orders.pk_sales_order_id as sales_order_id,
            sales_order_details.pk_sales_order_detail_id as sales_order_detail_id,
            sales_order_details.product_id as product_id,
            sales_orders.salesorder_status as order_status,
            sales_territory.name as territory,
            country_region.name as country,
            sales_order_details.order_qty as order_quantity,
            sales_order_details.unit_price as unit_price
        from sales_orders
        inner join sales_order_details on sales_orders.pk_sales_order_id = sales_order_details.fk_sales_order_id
        inner join sales_territory on sales_orders.fk_territory_id = sales_territory.pk_territory_id
        inner join state_province on sales_territory.pk_territory_id = state_province.fk_territory_id
        inner join country_region on state_province.fk_country_region_code = country_region.pk_country_region_code
        where sales_orders.salesorder_status in ('2', '5') -- Adjust if the statuses are numbers or strings
    ),
    
    metrics as (
        select
            sales_order_id,
            territory,
            country,
            count(distinct sales_order_id) as total_orders,
            cast(sum(order_quantity * unit_price) as numeric(18,2)) as total_sales
        from joined
        group by             
            sales_order_id,
            territory,
            country
    )
     
select * 
from metrics
