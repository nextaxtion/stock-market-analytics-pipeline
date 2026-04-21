# Data Model — Stock Market Analytics Pipeline

> BigQuery project: `dezoomcamp-486216`  
> Dataset: `dezoomcampds`  
> Total rows: ~26.2 million price records, 8,049 tickers, 1962–2020

---

## The 4 Layers

```
GCS (raw files)
      ↓  load_to_bigquery.py
  LAYER 0: RAW          (2 tables — untouched source data)
      ↓  dbt staging
  LAYER 1: STAGING      (2 views — cleaned & renamed)
      ↓  dbt core
  LAYER 2: STAR SCHEMA  (2 tables — normalized for analysis)
      ↓  dbt aggregations
  LAYER 3: AGGREGATIONS (2 tables — pre-summed for dashboard)
```

---

## Full Lineage

```
GCS Parquet ──► raw_daily_prices ──► stg_daily_prices (view) ──┐
                                                                 ├──► fact_daily_prices ──► agg_sector_performance
GCS CSV ──────► raw_symbols_meta ──► stg_symbols_meta (view) ──┘         │                  agg_monthly_summary
                                                │                         │                  agg_yearly_performance
                                                └──► dim_companies ◄──────┘ (FK: symbol)     agg_market_crash_analysis
                                                                                              agg_top_tickers_alltime
```

---

## Layer 0 — Raw Tables

Loaded by `scripts/load_to_bigquery.py` directly from GCS. dbt does NOT modify these tables — they are the raw inputs to the pipeline.

---

### `raw_daily_prices` — 26,228,008 rows

**Source:** `gs://dezoomcampstore/processed/stock_prices.parquet/`  
**Format:** Parquet (Spark output)  
**Design:** Partitioned by `trade_date` (MONTH), Clustered by `symbol`

Every row = one stock/ETF ticker on one trading day.

| Column | Type | Example | Description |
|---|---|---|---|
| `symbol` | STRING | `AAPL` | Ticker symbol (extracted from CSV filename by Spark) |
| `trade_date` | DATE | `2019-03-15` | The trading day |
| `open` | FLOAT64 | `183.72` | Price when the market opened that day |
| `high` | FLOAT64 | `187.16` | Highest price reached during the day |
| `low` | FLOAT64 | `183.32` | Lowest price reached during the day |
| `close` | FLOAT64 | `186.12` | Price when the market closed — the most referenced price |
| `adj_close` | FLOAT64 | `185.90` | Close price adjusted for dividends and stock splits |
| `volume` | INT64 | `24285900` | Number of shares traded that day |
| `daily_return` | FLOAT64 | `0.0134` | % change from previous close: `(close - prev_close) / prev_close` |
| `moving_avg_30` | FLOAT64 | `180.50` | Rolling average of close over last 30 trading days (short-term trend) |
| `moving_avg_60` | FLOAT64 | `176.20` | Rolling average of close over last 60 trading days (medium-term trend) |
| `volatility_30` | FLOAT64 | `0.0082` | Rolling standard deviation of `daily_return` over 30 days (risk measure) |
| `week52_high` | FLOAT64 | `233.47` | Highest "high" price over last 252 trading days (~1 year) |
| `week52_low` | FLOAT64 | `142.00` | Lowest "low" price over last 252 trading days (~1 year) |
| `security_name` | STRING | `Apple Inc.` | Company name (joined from metadata by Spark) |
| `listing_exchange` | STRING | `Q` | Raw exchange code |
| `market_category` | STRING | `NASDAQ Global Select Market` | Human-readable market segment (mapped from raw code by Spark) |
| `market_category_code` | STRING | `Q` | Raw category code (Q/G/S/N/A) |
| `etf_flag` | STRING | `N` | Raw ETF flag from metadata CSV |
| `is_etf` | BOOLEAN | `false` | True if this ticker is an ETF |

**Why partitioned by MONTH?**  
Most analytical queries filter by date range (e.g. "show 2010 data"). BigQuery skips entire partitions outside the filter → instead of scanning 26M rows it scans only the relevant months.  
DAY granularity was not used because 60 years × 252 trading days = ~15,000 partitions, which exceeds BigQuery's limit. MONTH = ~720 partitions, well within limits.

**Why clustered by `symbol`?**  
Within each monthly partition, rows are physically sorted by symbol. A query `WHERE symbol = 'AAPL'` skips to Apple's rows directly instead of scanning every ticker in the partition.

---

### `raw_symbols_meta` — 8,049 rows

**Source:** `gs://dezoomcampstore/security-market-raw-data/symbols_valid_meta.csv`  
**Format:** CSV  
**Design:** Flat table (small lookup table, no partitioning needed)

One row per ticker. The "dictionary" that describes every symbol.

| Column | Type | Example | Description |
|---|---|---|---|
| `Symbol` | STRING | `AAPL` | Ticker symbol |
| `Security Name` | STRING | `Apple Inc.` | Full company or fund name |
| `Nasdaq Traded` | STRING | `Y` | Whether the ticker trades on NASDAQ |
| `Listing Exchange` | STRING | `Q` | Exchange code |
| `Market Category` | STRING | `Q` | Raw category code (Q=NASDAQ Global Select, G=NASDAQ Global, S=NASDAQ Capital, N=NYSE, A=NYSE American) |
| `ETF` | STRING | `N` | Y/N flag |
| `Round Lot Size` | FLOAT64 | `100` | Standard trading lot size |
| `Test Issue` | STRING | `N` | Y if this is a test/dummy ticker |
| `Financial Status` | STRING | `N` | N=Normal, D=Deficient, E=Delinquent |
| `CQS Symbol` | STRING | `AAPL` | Consolidated Quote System symbol |
| `NASDAQ Symbol` | STRING | `AAPL` | NASDAQ-specific symbol |
| `NextShares` | STRING | `N` | Y if this is a NextShares fund |

> **Note:** Column names contain spaces (e.g. `Security Name`). In BigQuery SQL these require backtick quoting: `` `Security Name` ``. The staging layer renames all columns to clean `snake_case`.

---

## Layer 1 — Staging Views

Thin cleaning layer written in dbt. Materialized as **SQL VIEWs** — they store no data. When queried, BigQuery runs the SELECT live against the raw tables. The purpose is to give downstream models clean, consistent column names so no model ever references raw column names with spaces or inconsistent casing.

---

### `stg_daily_prices` (VIEW)

**Source:** `raw_daily_prices`  
**dbt file:** `dbt/models/staging/stg_daily_prices.sql`

What it does:
- Selects all useful columns with clean aliases
- Filters out rows where `close IS NULL`, `close <= 0`, `trade_date IS NULL`, or `symbol IS NULL`/empty
- No renaming needed (Spark already used snake_case)

---

### `stg_symbols_meta` (VIEW)

**Source:** `raw_symbols_meta`  
**dbt file:** `dbt/models/staging/stg_symbols_meta.sql`

What it does:
- Renames all columns from `` `Security Name` `` → `security_name` (removes spaces, lowercase)
- Maps raw category codes to human-readable labels: `Q` → `NASDAQ Global Select Market`, etc.
- Trims whitespace from string columns
- Filters out null or empty symbols
- Excludes test issues (`Test Issue = Y`)

---

## Layer 2 — Star Schema (Core)

The industry-standard pattern for analytical data warehouses. One **fact table** holds measurements; **dimension tables** hold descriptive attributes. Fact and dimension tables join on shared keys.

```
        dim_companies          ← "WHO is this ticker?"
        (8,049 rows)
              │
              │  symbol = symbol  (Foreign Key)
              │
    fact_daily_prices          ← "WHAT happened on this day?"
    (26,228,008 rows)
              │
              │  trade_date      (Partition key)
              │  symbol          (Cluster key)
```

---

### `dim_companies` — 8,049 rows

**Materialization:** TABLE  
**dbt file:** `dbt/models/core/dim_companies.sql`  
**Source:** `stg_symbols_meta`

The **dimension** table. Answers: *"Tell me everything about this ticker symbol."*  
One row per symbol. No prices, no dates — only descriptive company information.

| Column | Type | Example | Description |
|---|---|---|---|
| `symbol` | STRING | `MSFT` | **Primary Key** — unique per row |
| `security_name` | STRING | `Microsoft Corporation` | Full company or fund name |
| `listing_exchange` | STRING | `Q` | Exchange code |
| `market_category_code` | STRING | `Q` | Raw category code |
| `market_category` | STRING | `NASDAQ Global Select Market` | Human-readable market segment |
| `financial_status` | STRING | `N` | N=Normal, D=Deficient, etc. |
| `is_etf` | STRING | `N` | Y/N flag |
| `nasdaq_traded` | STRING | `Y` | Y/N flag |
| `is_test_issue` | STRING | `N` | Y/N flag |
| `round_lot_size` | FLOAT64 | `100` | Standard trading lot size |

**Why a separate dimension table?**  
Without it, "Microsoft Corporation" would be stored ~13,000 times in the fact table (once per trading day over many years). The dimension table stores it once and the fact table stores only the `symbol` key. This follows the **DRY principle** in data modeling: one source of truth for company info.

**Deduplication:** The raw metadata may have duplicate symbols. The model uses `QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY security_name) = 1` to keep exactly one row per symbol.

---

### `fact_daily_prices` — 26,228,008 rows

**Materialization:** TABLE, Partitioned by `trade_date` (MONTH), Clustered by `symbol`  
**dbt file:** `dbt/models/core/fact_daily_prices.sql`  
**Source:** `stg_daily_prices`

The **fact table**. The center of the star. Answers: *"What happened to this stock on this date?"*  
Every row = one ticker on one trading day.

| Column | Type | Description |
|---|---|---|
| `trade_date` | DATE | Trading day — **partition key** |
| `symbol` | STRING | Ticker — **cluster key**, **FK → dim_companies.symbol** |
| `open` | FLOAT64 | Opening price |
| `high` | FLOAT64 | Intraday high |
| `low` | FLOAT64 | Intraday low |
| `close` | FLOAT64 | Closing price |
| `adj_close` | FLOAT64 | Adjusted closing price |
| `volume` | INT64 | Shares traded |
| `daily_return` | FLOAT64 | `(close - prev_close) / prev_close` |
| `moving_avg_30` | FLOAT64 | 30-day rolling average close |
| `moving_avg_60` | FLOAT64 | 60-day rolling average close |
| `volatility_30` | FLOAT64 | 30-day rolling stddev of daily_return |
| `week52_high` | FLOAT64 | 52-week high |
| `week52_low` | FLOAT64 | 52-week low |
| `market_category` | STRING | Denormalized from Spark join (convenience — avoids JOIN for simple queries) |
| `market_category_code` | STRING | Raw category code |
| `is_etf` | BOOLEAN | True if ETF |

**Joining with dim_companies:**
```sql
SELECT
    f.trade_date,
    f.symbol,
    d.security_name,
    d.market_category,
    f.close,
    f.daily_return
FROM `dezoomcamp-486216.dezoomcampds.fact_daily_prices` f
JOIN `dezoomcamp-486216.dezoomcampds.dim_companies` d
  ON f.symbol = d.symbol
WHERE f.trade_date BETWEEN '2019-01-01' AND '2019-12-31'
  AND d.market_category = 'NYSE'
```

---

## Layer 3 — Aggregations

Pre-computed monthly summaries designed for dashboard performance. Instead of Looker Studio scanning 26M rows on every chart refresh, it reads from these small tables.

---

### `agg_sector_performance` — 2,527 rows

**Materialization:** TABLE  
**dbt file:** `dbt/models/aggregations/agg_sector_performance.sql`  
**Source:** `fact_daily_prices`

One row per **(market_category × month)**.  
→ Powers **Looker Tile 1: Categorical distribution** — *"Which market segment performed best each month?"*

| Column | Type | Example | Description |
|---|---|---|---|
| `trade_month` | DATE | `2010-01-01` | First day of the month |
| `market_category` | STRING | `NYSE` | Market segment |
| `avg_daily_return` | FLOAT64 | `0.000830` | Average % return across all tickers in this category this month |
| `avg_volatility` | FLOAT64 | `0.014200` | Average 30-day volatility (risk indicator) |
| `avg_close` | FLOAT64 | `32.47` | Average closing price across tickers |
| `total_volume` | INT64 | `8400000000` | Total shares traded in this category this month |
| `ticker_count` | INT64 | `1204` | Distinct tickers that traded this month in this category |

**Sample insight:** In September 2008 (financial crisis), `avg_daily_return` for NYSE would be strongly negative and `avg_volatility` would spike — this is directly visible in a bar/line chart.

---

### `agg_monthly_summary` — 700 rows

**Materialization:** TABLE  
**dbt file:** `dbt/models/aggregations/agg_monthly_summary.sql`  
**Source:** `fact_daily_prices`

One row per **month** across the entire market. 60 years × 12 months = ~720 rows.  
→ Powers **Looker Tile 2: Temporal trend** — *"How did the overall market move from 1962 to 2020?"*

| Column | Type | Example | Description |
|---|---|---|---|
| `trade_month` | DATE | `2008-09-01` | **Primary Key** — first day of month |
| `avg_close` | FLOAT64 | `28.11` | Market-wide average closing price |
| `total_volume` | INT64 | `120000000000` | Total shares traded — all tickers, all days this month |
| `avg_daily_return` | FLOAT64 | `-0.009400` | Market dropped ~0.94% on average per day (Sept 2008 crash) |
| `avg_volatility` | FLOAT64 | `0.041000` | Very high — 2008 crisis |
| `active_ticker_count` | INT64 | `7203` | Distinct tickers that traded this month |
| `top_gainer_symbol` | STRING | `GLD` | Ticker with highest avg daily return this month |
| `top_loser_symbol` | STRING | `LEH` | Ticker with lowest avg daily return (Lehman Brothers, Sept 2008) |

**Historical markers visible in this table:**
- **1987 Black Monday** → `avg_daily_return` spike downward in October 1987
- **Dot-com crash (2000–2002)** → `avg_close` collapses over 3 years
- **2008 Financial Crisis** → `avg_volatility` spikes, `top_loser_symbol = LEH` in September 2008
- **2020 COVID (March 2020)** → sharp `avg_daily_return` drop, data ends April 2020

---

## Data Quality Tests (dbt)

All tests defined in `dbt/tests/schema.yml` and run with `dbt test --profiles-dir .`.

| Test | Table | Column | Type | Result |
|---|---|---|---|---|
| not_null | stg_daily_prices | symbol, trade_date, close | error | ✅ PASS |
| accepted_values | stg_daily_prices | market_category | warn | ✅ PASS |
| not_null + unique | stg_symbols_meta | symbol | error | ✅ PASS |
| not_null | dim_companies | symbol, security_name, market_category | error | ✅ PASS |
| unique | dim_companies | symbol | error | ✅ PASS |
| not_null | fact_daily_prices | symbol, trade_date, close, volume | error | ✅ PASS |
| relationships | fact_daily_prices | symbol → dim_companies | warn | ⚠️ WARN (expected) |
| not_null | agg_sector_performance | trade_month, market_category, ticker_count | error | ✅ PASS |
| not_null + unique | agg_monthly_summary | trade_month | error | ✅ PASS |
| not_null | agg_monthly_summary | avg_close, total_volume | error | ✅ PASS |
| not_null | agg_yearly_performance | trade_year, market_category, avg_daily_return | error | ✅ PASS |
| not_null + unique | agg_market_crash_analysis | event_name | error | ✅ PASS |
| not_null | agg_market_crash_analysis | avg_volatility | error | ✅ PASS |
| not_null + unique | agg_top_tickers_alltime | symbol | error | ✅ PASS |
| not_null | agg_top_tickers_alltime | avg_daily_return, trading_days | error | ✅ PASS |

> **WARN on FK relationship:** 21,518 price rows reference symbols that have no entry in `raw_symbols_meta`. These are historical tickers that were delisted before NASDAQ published the metadata file. They are valid price records — just missing company info. Set to `warn` (not `error`) intentionally.

---

## Quick Reference — All 11 Tables

| Table | Layer | Type | Rows | Partitioned | Clustered |
|---|---|---|---|---|---|
| `raw_daily_prices` | Raw | TABLE | 26.2M | trade_date (MONTH) | symbol |
| `raw_symbols_meta` | Raw | TABLE | 8,049 | — | — |
| `stg_daily_prices` | Staging | VIEW | 26.2M | — | — |
| `stg_symbols_meta` | Staging | VIEW | 8,049 | — | — |
| `dim_companies` | Core | TABLE | 8,049 | — | — |
| `fact_daily_prices` | Core | TABLE | 26.2M | trade_date (MONTH) | symbol |
| `agg_sector_performance` | Aggregation | TABLE | 2,527 | — | — |
| `agg_monthly_summary` | Aggregation | TABLE | ~720 | — | — |
| `agg_yearly_performance` | Aggregation | TABLE | ~209 | — | — |
| `agg_market_crash_analysis` | Aggregation | TABLE | 4 | — | — |
| `agg_top_tickers_alltime` | Aggregation | TABLE | ~6,000 | — | — |
