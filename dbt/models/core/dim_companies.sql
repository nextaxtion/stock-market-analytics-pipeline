/*
  dim_companies.sql — Company Dimension Table (Star Schema)
  ==========================================================

  What is a Dimension Table?
  --------------------------
  In a star schema, dimension tables hold the "descriptive attributes" —
  things that describe WHO or WHAT something is.
  Our dim_companies answers: "Tell me about this ticker symbol."
  It has ONE row per symbol. No prices, no dates — just company info.

  The fact table (fact_daily_prices) stores the actual measurements
  (prices, volume, returns) and joins to dim_companies via symbol.

  Star schema pattern:
      fact_daily_prices
          ↓ symbol (FK)
      dim_companies  ←  this table

  Why deduplicate?
  ----------------
  The raw metadata CSV may have duplicate rows for the same symbol
  (e.g. if a ticker re-listed). QUALIFY ROW_NUMBER()=1 keeps only the
  first (alphabetically by security_name) occurrence per symbol.

  BigQuery config:
  ----------------
  Materialized as TABLE (not view) — this table is joined frequently
  by fact_daily_prices queries. A persisted table is much faster than
  a view that re-runs the query on every join.
*/

{{
    config(
        materialized='table',
        description='Company dimension table — one row per ticker symbol'
    )
}}

with source as (
    select * from {{ ref('stg_symbols_meta') }}
),

-- Deduplicate: keep one row per symbol
deduped as (
    select *
    from source
    qualify row_number() over (
        partition by symbol
        order by security_name
    ) = 1
)

select
    symbol,
    security_name,
    listing_exchange,
    market_category_code,
    market_category,
    financial_status,
    is_etf,
    nasdaq_traded,
    is_test_issue,
    round_lot_size
from deduped
