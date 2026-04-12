/*
  agg_sector_performance.sql — Monthly performance metrics by market category
  ============================================================================

  What is an Aggregation Layer?
  ------------------------------
  Aggregation models are pre-computed summaries designed for dashboards.
  Instead of Looker Studio scanning 28M rows every time you load a chart,
  it queries this small aggregated table (~720 rows) instead.

  This model answers: "How did each market category perform each month?"

  Columns produced:
    market_category   : Human-readable category (NASDAQ Global Select Market, NYSE, etc.)
    trade_month       : First day of the month (e.g. 2019-01-01)
    avg_daily_return  : Average % return across all tickers in this category that month
    avg_volatility    : Average 30-day volatility (risk level) across tickers this month
    avg_close         : Average closing price across all tickers this month
    total_volume      : Total shares traded for all tickers in this category this month
    ticker_count      : Number of distinct tickers that traded in this category this month

  This table powers Looker Studio "Tile 1: Categorical distribution"
    - Bar chart: which market category has the highest average return?
    - You can filter by year and compare categories side by side

  Join pattern:
    fact_daily_prices → JOIN dim_companies ON symbol
    We join to get market_category from the dimension table (normalized approach)
    BUT since we denormalized market_category into fact_daily_prices (via Spark),
    we can also just use fact_daily_prices directly. We use the fact table column
    since it's already there (avoids extra JOIN).
*/

{{
    config(
        materialized='table',
        description="Monthly performance aggregated by market category. Powers the categorical dashboard tile."
    )
}}

with daily as (
    select * from {{ ref('fact_daily_prices') }}
),

monthly_agg as (
    select
        -- Truncate trade_date to the first day of the month
        -- e.g. 2019-03-15 → 2019-03-01
        -- This groups all days in the same month together
        date_trunc(trade_date, month)   as trade_month,

        market_category,

        -- AVG daily_return across all tickers in this category this month
        -- Positive = category generally went up; Negative = generally went down
        round(avg(daily_return), 6)     as avg_daily_return,

        -- AVG volatility — higher means the category was more "risky" that month
        round(avg(volatility_30), 6)    as avg_volatility,

        -- Typical price level for this category this month
        round(avg(close), 4)            as avg_close,

        -- Total trading activity for this category this month (in billions)
        sum(volume)                     as total_volume,

        -- How many distinct tickers were active in this category this month
        count(distinct symbol)          as ticker_count

    from daily

    -- Exclude rows where we don't have a valid category or return metric
    where market_category is not null
      and market_category != ''
      and daily_return is not null

    group by 1, 2
)

select
    trade_month,
    market_category,
    avg_daily_return,
    avg_volatility,
    avg_close,
    total_volume,
    ticker_count
from monthly_agg
order by trade_month, market_category
