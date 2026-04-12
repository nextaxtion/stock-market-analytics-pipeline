/*
  stg_daily_prices.sql — Staging layer for daily price data
  ==========================================================

  What does the staging layer do?
  --------------------------------
  Staging models are thin cleaning layers. They:
    1. Rename columns to consistent snake_case names
    2. Cast columns to correct data types
    3. Filter out obviously bad data (nulls, zeros)
    4. Add no business logic — keep it simple and predictable

  Why views?
  ----------
  Staging models are materialized as VIEWs (configured in dbt_project.yml).
  A view doesn't store data — it's just a saved SQL query. When you query it,
  BigQuery runs the SELECT against the raw table fresh each time.
  This is fine for staging since downstream models (tables) cache the result.

  Lineage: raw_daily_prices (source) → stg_daily_prices (view) → fact_daily_prices (table)
*/

with source as (
    select * from {{ source('stock_market_raw', 'raw_daily_prices') }}
),

cleaned as (
    select
        -- Primary identifiers
        symbol,
        trade_date,

        -- Price columns (already FLOAT64, just aliasing for clarity)
        open,
        high,
        low,
        close,
        adj_close,
        volume,

        -- Spark-computed metrics (computed with window functions)
        daily_return,
        moving_avg_30,
        moving_avg_60,
        volatility_30,
        week52_high,
        week52_low,

        -- Company metadata joined in Spark
        security_name,
        listing_exchange,
        market_category,
        market_category_code,
        etf_flag,
        is_etf

    from source

    -- Filter out rows with missing or zero close price
    -- (invalid data that slipped through the Spark filter)
    where close is not null
      and close > 0
      and trade_date is not null
      and symbol is not null
      and symbol != ''
)

select * from cleaned
