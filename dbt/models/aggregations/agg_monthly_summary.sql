/*
  agg_monthly_summary.sql — Market-wide monthly summary
  =======================================================

  Purpose:
  --------
  A single-row-per-month summary of the entire market.
  This table powers Looker Studio "Tile 2: Temporal trend" —
  a line chart showing how the overall market moved across 60 years.

  Columns produced:
    trade_month         : First day of month (e.g. 1990-01-01)
    avg_close           : Average closing price across ALL tickers this month
    total_volume        : Total shares traded across all tickers this month
    avg_daily_return    : Average daily return (market direction indicator)
    avg_volatility      : Average volatility (market stress indicator)
    active_ticker_count : Number of distinct tickers that traded this month
    top_gainer_symbol   : Ticker with highest avg daily return this month
    top_loser_symbol    : Ticker with lowest avg daily return this month

  This table has 1 row per month → ~720 rows for 60 years.
  Looker Studio line chart: X = trade_month, Y = avg_close or total_volume.

  Interesting insights to see:
    - Dot-com crash: avg_close collapses 2000–2002
    - 2008 financial crisis: huge volatility spike, avg_close drops
    - 2020 COVID crash: sharp dip in March 2020 (our data ends April 2020)
    - 1990s bull run: sustained upward trend in avg_close and volume

  Implementation note on top_gainer/top_loser:
  --------------------------------------------
  BigQuery doesn't have a direct FIRST() aggregate. We use ARRAY_AGG with
  ORDER BY and LIMIT 1 inside a subquery to get the single best/worst ticker.
  This is a common BigQuery pattern for "top-N per group" queries.
*/

{{
    config(
        materialized='table',
        description="Market-wide monthly summary. Powers the temporal trend dashboard tile."
    )
}}

with daily as (
    select * from {{ ref('fact_daily_prices') }}
),

-- Step 1: compute per-ticker, per-month averages
-- We need this to find the top gainer and top loser each month
ticker_monthly as (
    select
        date_trunc(trade_date, month)   as trade_month,
        symbol,
        avg(daily_return)               as avg_return_this_month

    from daily
    where daily_return is not null

    group by 1, 2
),

-- Step 2: aggregate to market-wide monthly stats
market_monthly as (
    select
        date_trunc(trade_date, month)   as trade_month,
        round(avg(close), 4)            as avg_close,
        sum(volume)                     as total_volume,
        round(avg(daily_return), 6)     as avg_daily_return,
        round(avg(volatility_30), 6)    as avg_volatility,
        count(distinct symbol)          as active_ticker_count

    from daily

    group by 1
),

-- Step 3: for each month, find the single best and worst ticker
-- ARRAY_AGG(symbol ORDER BY avg_return DESC LIMIT 1)[OFFSET(0)]
--   → sorts tickers by their avg_return descending, takes the first one
-- This is BigQuery's idiomatic way to do "argmax" in SQL

top_gainers as (
    select
        trade_month,
        array_agg(symbol order by avg_return_this_month desc limit 1)[offset(0)] as top_gainer_symbol
    from ticker_monthly
    group by trade_month
),

top_losers as (
    select
        trade_month,
        array_agg(symbol order by avg_return_this_month asc limit 1)[offset(0)] as top_loser_symbol
    from ticker_monthly
    group by trade_month
)

-- Step 4: join everything together
select
    m.trade_month,
    m.avg_close,
    m.total_volume,
    m.avg_daily_return,
    m.avg_volatility,
    m.active_ticker_count,
    g.top_gainer_symbol,
    l.top_loser_symbol

from market_monthly    m
left join top_gainers  g using (trade_month)
left join top_losers   l using (trade_month)

order by m.trade_month
