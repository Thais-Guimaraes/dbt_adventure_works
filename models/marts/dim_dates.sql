with recursive 
    calendar_dates as (
        select datefromparts(1990, 1, 1) as calendar_date
        union all
        select dateadd(day, 1, calendar_date)
        from calendar_dates
        where calendar_date < '2035-01-01'
    )
select
    calendar_date as "DATE"
    , year(calendar_date) as "YEAR"
    , month(calendar_date) as "MONTH"
    , monthname(calendar_date) as month_name
    , day(calendar_date) as "DAY"
    , dayname(calendar_date) as day_name
    , quarter(calendar_date) as "QUARTER"
    , week(calendar_date) as "WEEK"
    , dayofmonth(calendar_date) as day_month
    , dayofweek(calendar_date) as day_week
    , dayofyear(calendar_date) as day_year
    , last_day (calendar_date) as "LAST_DAY"
    , last_day (calendar_date, 'quarter') as last_day_quarter
    , last_day (calendar_date, 'year') as last_day_year
    , last_day (calendar_date, 'week') as last_day_week
    , weekofyear(calendar_date) as week_year
    , yearofweek(calendar_date) as year_week
    , yearofweekiso(calendar_date) as year_week_iso
    , weekiso(calendar_date) as week_iso
    , dayofweekiso(calendar_date) as day_week_iso
    , ceil(dayofmonth(calendar_date)/7) as week_month

, case
    when quarter(calendar_date) = 1 then datefromparts(year(calendar_date), 1, 1)
    when quarter(calendar_date) = 2 then datefromparts(year(calendar_date), 4, 1)
    when quarter(calendar_date) = 3 then datefromparts(year(calendar_date), 7, 1)
    else datefromparts(year(calendar_date), 10, 1)
    end as first_day_quarter
    , datediff (day, first_day_quarter, "DATE") + 1 as day_quarter
    , datediff (day, first_day_quarter, last_day_quarter) + 1 as days_quarter
    , ceil(day_quarter/7) as week_quarter

from calendar_dates