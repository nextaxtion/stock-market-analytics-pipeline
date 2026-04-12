/*
  stg_symbols_meta.sql — Staging layer for ticker metadata
  =========================================================

  Source: raw_symbols_meta (loaded from symbols_valid_meta.csv on GCS)

  Key challenge: Column names in BigQuery have spaces (e.g. "Security Name",
  "Market Category"). In BigQuery SQL, columns with spaces must be wrapped in
  backticks: `Security Name`. We rename them here to clean snake_case names
  so all downstream models use consistent, readable column names.

  The MARKET_CATEGORY_MAP translates raw codes to human-readable labels:
    Q → NASDAQ Global Select Market
    G → NASDAQ Global Market
    S → NASDAQ Capital Market
    N → NYSE
    A → NYSE American (AMEX)
  (This complements the mapping already done in Spark for the prices table)
*/

with source as (
    select * from {{ source('stock_market_raw', 'raw_symbols_meta') }}
),

cleaned as (
    select
        -- Use backticks for columns with spaces
        trim(`Symbol`)              as symbol,
        trim(`Security Name`)       as security_name,
        trim(`Listing Exchange`)    as listing_exchange,
        trim(`Market Category`)     as market_category_code,
        trim(`Financial Status`)    as financial_status,
        trim(`CQS Symbol`)          as cqs_symbol,
        trim(`NASDAQ Symbol`)       as nasdaq_symbol,

        -- Boolean flags
        `ETF`                       as is_etf,
        `Nasdaq Traded`             as nasdaq_traded,
        `Test Issue`                as is_test_issue,
        `NextShares`                as is_nextshares,

        -- Numeric
        `Round Lot Size`            as round_lot_size,

        -- Map raw category codes to human-readable labels
        case trim(`Market Category`)
            when 'Q' then 'NASDAQ Global Select Market'
            when 'G' then 'NASDAQ Global Market'
            when 'S' then 'NASDAQ Capital Market'
            when 'N' then 'NYSE'
            when 'A' then 'NYSE American'
            else 'Other'
        end                         as market_category

    from source

    -- Only keep real, active securities (exclude test issues)
    where `Symbol` is not null
      and trim(`Symbol`) != ''
)

select * from cleaned
