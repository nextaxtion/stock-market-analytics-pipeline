/*
  agg_market_crash_analysis.sql — Market behaviour during key crash events
  =========================================================================

  Purpose:
  --------
  Pre-labels 4 well-known market crisis periods and computes aggregate
  volatility, return, and volume for each — ready for a side-by-side bar chart
  comparison with zero Looker Studio configuration.

  Chart recipe (Looker Studio):
    Chart type  : Bar chart (horizontal recommended)
    Dimension   : event_name
    Metric 1    : avg_volatility    (how panicked was the market?)
    Metric 2    : avg_daily_return  (how hard did prices fall?)
    Metric 3    : total_volume      (how much panic selling?)
    No filter needed — each event is already a single row.

  Events covered:
    Black Monday (1987)         : Single-day 22% crash, worst in history
    Dot-com Crash (2000–2002)   : NASDAQ fell 78% from peak
    Financial Crisis (2007–2009): Subprime meltdown, S&P -57%
    COVID Crash (2020)          : Fastest ever 30% drop in 22 days

  Rows: exactly 4 — one per event.
*/

{{
    config(
        materialized='table',
        description="Market behaviour during 4 major crash events. Powers the crisis comparison bar chart."
    )
}}

with events as (
    -- Define each crisis window by start/end date
    -- Pre/post buffers are intentionally narrow so we capture the acute phase
    select 'Black Monday (1987)'        as event_name, date('1987-09-01') as event_start, date('1987-12-31') as event_end
    union all
    select 'Dot-com Crash (2000–2002)'  as event_name, date('2000-03-01') as event_start, date('2002-10-31') as event_end
    union all
    select 'Financial Crisis (2007–09)' as event_name, date('2007-10-01') as event_start, date('2009-03-31') as event_end
    union all
    select 'COVID Crash (2020)'         as event_name, date('2020-01-20') as event_start, date('2020-04-01') as event_end
),

fact as (
    select * from {{ ref('fact_daily_prices') }}
    where daily_return is not null
)

select
    e.event_name,
    e.event_start                                           as period_start,
    e.event_end                                             as period_end,

    -- Average daily return during the event — negative = market falling
    round(avg(f.daily_return), 6)                           as avg_daily_return,

    -- Average volatility — spikes during crashes (fear index proxy)
    round(avg(f.volatility_30), 6)                          as avg_volatility,

    -- Total shares traded — volume surges in panic sell-offs
    sum(f.volume)                                           as total_volume,

    -- How many tickers were affected (usually most of the market)
    count(distinct f.symbol)                                as affected_tickers,

    -- Duration in calendar days
    date_diff(e.event_end, e.event_start, day)              as event_duration_days

from events e
join fact f
    on f.trade_date between e.event_start and e.event_end

group by 1, 2, 3
order by e.event_start
