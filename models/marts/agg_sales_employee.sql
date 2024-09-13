with 
    -- agregação dos dados dos vendedores
    vendedores as (
        select
            cast(pk_sales_person_id as varchar) as id_vendedor,
            cast(territory_id as varchar) as id_territorio,
            sales_quota as cota_vendas,
            sales_ytd as vendas_ano_atual
        from {{ ref('stg_sales_person') }}
    ),

    -- agregação dos dados de pessoas, filtrando apenas vendedores
    pessoas as (
        select
            person_fullname as vendedor,
            pk_person_business_entity_id as id_entid_comercial_pessoa
        from {{ ref('stg_person') }}
        where person_type = 'SP'
    ),

    -- cálculo das métricas de vendas a partir dos detalhes de pedidos
    metricas_vendas as (
        select
            joined.fk_salesperson as fk_vendedor,
            sum(order_quantity) as total_quantidade_vendida,
            cast(sum((unit_price * (1 - unit_price_discount) * order_quantity)) as numeric(18,2)) as total_vendas,
            cast(sum(unit_price * order_quantity) - sum((unit_price * (1 - unit_price_discount) * order_quantity)) as numeric(18,2)) as total_desconto,        
            cast(count(distinct fk_order) as int) as total_pedidos,
            cast(count(distinct fk_customer) as int) as total_clientes,
            cast(round(avg(net_total), 2) as numeric(18,2)) as valor_medio_pedido
        from {{ ref('int_orders_details') }} joined
        group by joined.fk_salesperson
    )

-- combinação dos dados de vendedores, métricas de vendas e pessoas
select
    vendedores.id_vendedor,
    vendedores.id_territorio,
    pessoas.vendedor,
    vendedores.cota_vendas,
    vendedores.vendas_ano_atual,
    metricas_vendas.total_quantidade_vendida,
    metricas_vendas.total_vendas,
    metricas_vendas.total_desconto,
    metricas_vendas.total_pedidos,
    metricas_vendas.total_clientes,
    metricas_vendas.valor_medio_pedido,
    case
        when metricas_vendas.total_vendas >= vendedores.cota_vendas then 'cota atingida'
        else 'cota não atingida'
    end as status_cota
from
    vendedores
left join
    metricas_vendas on vendedores.id_vendedor = metricas_vendas.fk_vendedor
left join
    pessoas on vendedores.id_vendedor = pessoas.id_entid_comercial_pessoa
