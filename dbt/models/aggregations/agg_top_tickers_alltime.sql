/*
  agg_top_tickers_alltime.sql — All-time best and worst performing stock tickers
  ===============================================================================

  Purpose:
  --------
  Ranks every stock (ETFs excluded) by average daily return over their full
  trading history — filtered to tickers with at least 252 trading days of data
  so short-lived or data-sparse tickers don't dominate the leaderboard.

  Chart recipe (Looker Studio):
    Top performers tile:
      Chart type  : Horizontal bar chart
      Dimension   : security_name  (or symbol if name is null)
      Metric      : avg_daily_return
      Sort        : avg_daily_return DESC
      Row limit   : 20
    Bottom performers tile:
      Same, but Sort: avg_daily_return ASC, Row limit 20

  Columns:
    symbol              : Ticker symbol (e.g. AAPL)
    security_name       : Company full name (from dim_companies)
    market_category     : Which market segment the stock belongs to
    trading_days        : Total days in the dataset for this ticker
    avg_daily_return    : Mean daily % return — the primary ranking metric
    avg_volatility      : Average risk level over the ticker's lifetime
    total_volume        : Lifetime cumulative shares traded
    first_trade_date    : Earliest date in our dataset for this ticker
    last_trade_date     : Most recent date (capped at 2020-04-01)
    approx_total_return : (1 + avg_daily_return)^trading_days - 1

  Rows: ~6,000–7,000 (all stocks with ≥252 trading days, ETFs excluded)
*/

{{
    config(
        materialized='table',
        description="All-time performance ranking for individual stock tickers (no ETFs). Powers the top/bottom performers leaderboard."
    )
}}

with ticker_stats as (
    select
        f.symbol,
        d.security_name,
        d.market_category,

        count(distinct f.trade_date)    as trading_days,
        round(avg(f.daily_return), 6)   as avg_daily_return,
        round(avg(f.volatility_30), 6)  as avg_volatility,
        sum(f.volume)                   as total_volume,
        min(f.trade_date)               as first_trade_date,
        max(f.trade_date)               as last_trade_date

    from {{ ref('fact_daily_prices') }} f
    inner join {{ ref('dim_companies') }} d
        on f.symbol = d.symbol

    where f.daily_return is not null
      and d.is_etf = false          -- stocks only — ETFs behave differently

    group by 1, 2, 3
)

select
    symbol,
    security_name,
    market_category,
    trading_days,
    avg_daily_return,
    avg_volatility,
    total_volume,
    first_trade_date,
    last_trade_date,

    -- Approximate lifetime total return. SAFE.POW returns NULL instead of
    -- overflowing on tickers with extreme compounded returns over many years.
    round(safe.pow(1.0 + avg_daily_return, trading_days) - 1.0, 4) as approx_total_return

from ticker_stats
where trading_days >= 252   -- at least 1 full year of data for statistical validity
order by avg_daily_return desc
