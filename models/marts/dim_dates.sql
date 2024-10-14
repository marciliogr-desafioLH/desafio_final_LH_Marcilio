with dates_raw as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2016-01-01' as date)"
    ) }}
),

days_info as (
    select
        cast(date_day as date) as date
        , extract(dayofweek from date_day) as day_of_week
        , floor((extract(dayofyear from date_day) - 1) / 7) + 1 as week_number
        , extract(day from date_day) as day_of_month
        , extract(month from date_day) as month_number
        , to_char(date_day, 'Month') as month_date
        , to_char(date_day, 'Mon') as month_name_abbr
        , to_char(date_day, 'Mon - YYYY') as month_year
        , extract(quarter from date_day) as quarter
        , extract(year from date_day) as year
        , extract(dayofyear from date_day) as day_of_year
        , floor((extract(day from date_day) - 1) / 7) + 1 as week_of_month
        , case
            when extract(month from date_day) in (12, 1, 2) then 'Winter'
            when extract(month from date_day) in (3, 4, 5) then 'Spring'
            when extract(month from date_day) in (6, 7, 8) then 'Summer'
            when extract(month from date_day) in (9, 10, 11) then 'Autumn'
        end as season
        , case
            when extract(dayofweek from date_day) between 2 and 6 then true
            else false
        end as is_business_day
        , case
            when (extract(month from date_day) = 12 and extract(day from date_day) = 25) then true -- Natal
            when (extract(month from date_day) = 1 and extract(day from date_day) = 1) then true -- Ano Novo
            else false
        end as is_holiday
    from dates_raw
)

select
    date
    , day_of_week
    , week_number
    , day_of_month
    , month_number
    , month_date
    , month_name_abbr
    , month_year
    , quarter
    , year
    , day_of_year
    , week_of_month
    , season
    , is_business_day
    , is_holiday
from days_info
