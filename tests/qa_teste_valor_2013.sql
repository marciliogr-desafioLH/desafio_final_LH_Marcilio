/* Teste de valor bruto no dia 01/01/2013 auditado pela equipe comercial */

{{ config (
        severity = 'error'
    )
}}

 with
    receita_bruta as (
        select 
            sum(unit_price) as valor_bruto
        from {{ ref("fact_sales") }}
        where order_date = '2013-01-01'
    )
select 
    valor_bruto
from receita_bruta
where valor_bruto between 19623.8966 and 19623.8976