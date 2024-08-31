with stg_employee as (
    select *
    from {{ref('stg_employee')}}
),

stg_sales_person as (
    select *
    from {{ref('stg_sales_person')}}
),

complete_table as (
    select
        {{ dbt_utils.generate_surrogate_key(
                    ['pk_employee_id']
                )
            }} as employee_sk -- genarate surrogate key
        , stg_employee.pk_employee_id
        , stg_employee.job_title
        , stg_employee.gender
        , stg_employee.hire_date
        , stg_sales_person.pk_sales_person_id
        , stg_sales_person.territory_id
        , stg_sales_person.sales_quota
        , stg_sales_person.bonus
        , stg_sales_person.sales_ytd
        , stg_sales_person.sales_last_year
    from stg_employee
    left join stg_sales_person
        on stg_sales_person.pk_sales_person_id = stg_employee.pk_employee_id
)

select *
from complete_table
