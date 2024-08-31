with date_range as (
    -- Gera as datas a partir de 2008-01-01 até 2025-12-31
    select 
        dateadd(day, seq4() - 1, '2008-01-01') as date
    from 
        table(generator(rowcount => 6574)) -- 6574 dias entre 2008-01-01 e 2025-12-31
)

select
    date::date as date
    , extract(year from date) as year
    , extract(month from date) as month
    , extract(day from date) as day
    , to_char(date, 'Month') as month_name
    , to_char(date, 'Day') as day_name
    , extract(quarter from date) as quarter
from 
    date_range
order by 
    date
