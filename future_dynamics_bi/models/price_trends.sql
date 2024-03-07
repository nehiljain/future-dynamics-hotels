with hotels_src as (

    select *,
    hotel_name || ' - Check-in: ' || checkin_date AS hotel_checkin
    from {{ ref('hotel_prices') }}
), dates_src as (
    select *
    from {{ ref('dates') }}
), buy_positions as (
    select 
    hotels_src.*,
    buy_positions.clean_list_price_usd as buy_price_usd
    from hotels_src
    left join {{ ref('buy_positions') }} as buy_positions
        on buy_positions.hotel_checkin = hotels_src.hotel_checkin
), min_max_prices as (
    select
        *,
        min(list_price_usd) over (partition by hotel_name, checkin_date, execution_at_dt order by hotel_name, checkin_date, execution_at_dt) as min_list_price_usd,
        max(list_price_usd) over (partition by hotel_name, checkin_date, execution_at_dt order by hotel_name, checkin_date, execution_at_dt) as max_list_price_usd,
    from buy_positions
), price_trends_by_day as (
    select 
        execution_at_dt,
        date_trunc('day', execution_at_dt) as exec_date,
        hotel_name,
        checkin_date,
        hotel_checkin,
        max(buy_price_usd) as buy_price_usd,
        min(min_list_price_usd) as min_list_price_usd,
        max(max_list_price_usd) as max_list_price_usd
    from min_max_prices
    group by execution_at_dt, hotel_name, checkin_date, hotel_checkin
)
select
    dates_src.*,
    price_trends_by_day.*,
    ROUND(((LAG(price_trends_by_day.min_list_price_usd) OVER (ORDER BY execution_at_dt) - price_trends_by_day.min_list_price_usd) / LAG(price_trends_by_day.min_list_price_usd) OVER (ORDER BY execution_at_dt)) * 100, 2) AS delta_pct_min_list_price,
from dates_src
left join price_trends_by_day
    on price_trends_by_day.exec_date = dates_src.dt
where dates_src.dt >= (select min(execution_at_dt) from price_trends_by_day)
order by dates_src.dt asc