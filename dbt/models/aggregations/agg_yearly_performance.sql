/*
  agg_yearly_performance.sql — Annual performance by market category
  ===================================================================

  Purpose:
  --------
  Annual summary grouped by market category. Powers a grouped bar chart
  showing which sectors (NASDAQ Global Select, NYSE, etc.) delivered better
  returns in each decade — with minimal Looker Studio configuration.

  Chart recipe (Looker Studio):
    Chart type  : Grouped bar chart
    Dimension   : trade_year
    Breakdown   : market_category
    Metric      : avg_daily_return  (or switch to avg_volatility for risk view)
    Filter      : optional — decade range slider

  Columns:
    trade_year          : 4-digit year (1962–2020)
    market_category     : Market segment (NASDAQ Global Select, NYSE, etc.)
    avg_daily_return    : Mean daily return for all tickers in this category/year
    avg_volatility      : Mean 30-day volatility — proxy for market stress
    total_volume        : Aggregate shares traded (shows explosion post-internet)
    ticker_count        : Number of distinct tickers active this year
    approx_annual_return: (1 + avg_daily_return)^252 - 1  — annualised estimate

  Rows: ~5 categories × ~59 years ≈ 295 rows
*/

{{
    config(
        materialized='table',
        description="Annual performance metrics by market category. Powers year-over-year sector comparison chart."
    )
}}

with daily as (
    select * from {{ ref('fact_daily_prices') }}
    where daily_return is not null
      and market_category is not null
      and market_category != ''
)

select
    extract(year from trade_date)           as trade_year,
    market_category,

    -- Average daily return this year across all tickers in this category
    round(avg(daily_return), 6)             as avg_daily_return,

    -- Average 30-day rolling volatility — higher = more stressed market
    round(avg(volatility_30), 6)            as avg_volatility,

    -- Total shares traded — enables the "volume explosion post-2000" visual
    sum(volume)                             as total_volume,

    -- How many unique tickers traded at some point this year
    count(distinct symbol)                  as ticker_count,

    -- Annualised return approximation: compound avg daily return over 252 trading days
    -- e.g. avg_daily_return=0.001 → approx_annual_return ≈ 0.282 (28.2% per year)
    round(pow(1.0 + avg(daily_return), 252) - 1.0, 4) as approx_annual_return

from daily

group by 1, 2
order by 1, 2
