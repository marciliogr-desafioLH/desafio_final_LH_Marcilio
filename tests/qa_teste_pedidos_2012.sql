/* Teste da quantidade de pedidos em 2012 auditado pela equipe comercial */

{{ config (
        severity = 'error'
    )
}}

 with
    pedidos as (
        select 
            sum(order_quantity) as total_pedidos
        from {{ ref("fact_sales") }}
        where order_date between '2012-01-01' and '2012-12-31'
    )
select 
    total_pedidos
from pedidos
where total_pedidos = 68579