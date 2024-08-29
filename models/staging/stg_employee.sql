with
    raw_data as (
        select
            cast(businessentityid as int) as pk_employee_id
            , cast(nationalidnumber as int) as national_id_number
            , cast(loginid as varchar) as login_id
            , cast(jobtitle as varchar) as job_title
            , cast(birthdate as date) as birth_date
            , cast(maritalstatus as varchar) as marital_status
            , cast(gender as varchar) as gender
            , cast(hiredate as date) as hire_date
            , cast(salariedflag as boolean) as salaried_flag
            , cast(vacationhours as int) as vacation_hours
            , cast(sickleavehours as int) as sick_leave_hours
            , cast(currentflag as boolean) as current_flag
            --unused columns:
            --rowguid 
            --modifieddate
            --organizationnode
        from {{ source('erp', 'employee') }}
    )
select *
from raw_data
