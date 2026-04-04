"""
PySpark Stock Market Processing Script
=======================================
Reads raw stock/ETF CSVs from local disk or GCS, enriches with window-based
metrics, joins company metadata, and writes partitioned Parquet back to GCS.

Usage
-----
# Test locally on a small sample of 50 tickers (fast, ~30s):
python3 spark/process_stock_data.py --mode local --sample 50

# Full local run (all 8000 tickers, needs ~6GB RAM):
python3 spark/process_stock_data.py --mode local

# Full run reading from / writing to GCS:
python3 spark/process_stock_data.py --mode gcs

Environment (for GCS mode)
-----------
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json
"""

import argparse
import glob as _glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GCS_BUCKET = "gs://dezoomcampstore"
RAW_BASE   = f"{GCS_BUCKET}/security-market-raw-data"
PROC_BASE  = f"{GCS_BUCKET}/processed"

LOCAL_DATA = os.path.expanduser("~/stock-market-analytics-pipeline/data")

PRICE_SCHEMA = T.StructType([
    T.StructField("Date",      T.StringType(), True),
    T.StructField("Open",      T.DoubleType(), True),
    T.StructField("High",      T.DoubleType(), True),
    T.StructField("Low",       T.DoubleType(), True),
    T.StructField("Close",     T.DoubleType(), True),
    T.StructField("Adj Close", T.DoubleType(), True),
    T.StructField("Volume",    T.LongType(),   True),
])

MARKET_CATEGORY_MAP = {
    "Q": "NASDAQ Global Select Market",
    "G": "NASDAQ Global Market",
    "S": "NASDAQ Capital Market",
    "N": "NYSE",
    " ": "Other",
    "":  "Other",
}


# ---------------------------------------------------------------------------
# Spark session builder
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StockMarketProcessing")
        .config("spark.driver.memory", "10g")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Step 1: Read CSVs -> add symbol + is_etf columns
# ---------------------------------------------------------------------------

def read_price_csvs(spark: SparkSession, path_glob: str, sample_n: int = 0):
    """
    Concept: input_file_name()
    --------------------------
    Spark tracks which file each row came from via input_file_name().
    We use a regex to extract the ticker from the filename:
        /data/stocks/AAPL.csv  ->  AAPL
        gs://dezoomcampstore/.../etfs/SPY.csv  ->  SPY

    sample_n > 0 (local mode only): limit to the first N files so we can
    validate logic quickly without loading all 8000 tickers into RAM.
    """
    if sample_n > 0:
        files = sorted(_glob.glob(path_glob))[:sample_n]
        if not files:
            raise FileNotFoundError(f"No files matched: {path_glob}")
        source = files
    else:
        source = path_glob

    return (
        spark.read
        .schema(PRICE_SCHEMA)
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .csv(source)
        .withColumn(
            "symbol",
            F.regexp_extract(F.input_file_name(), r"/([^/]+)\.csv$", 1)
        )
        .withColumn(
            "is_etf",
            F.input_file_name().contains("/etfs/").cast(T.BooleanType())
        )
    )


# ---------------------------------------------------------------------------
# Step 2: Clean & cast
# ---------------------------------------------------------------------------

def clean(df):
    return (
        df
        .withColumnRenamed("Date",      "trade_date")
        .withColumnRenamed("Open",      "open")
        .withColumnRenamed("High",      "high")
        .withColumnRenamed("Low",       "low")
        .withColumnRenamed("Close",     "close")
        .withColumnRenamed("Adj Close", "adj_close")
        .withColumnRenamed("Volume",    "volume")
        .withColumn("trade_date", F.to_date("trade_date", "yyyy-MM-dd"))
        .filter(F.col("trade_date").isNotNull())
        .filter(F.col("close").isNotNull() & (F.col("close") > 0))
        .filter(F.col("symbol") != "")
    )


# ---------------------------------------------------------------------------
# Step 3: Window-based derived metrics
# ---------------------------------------------------------------------------

def add_metrics(df):
    """
    Concept: Window Functions
    -------------------------
    A Window defines a sliding frame over sorted rows within a group.

    PARTITION BY symbol -> each ticker gets its own independent window.
    ORDER BY trade_date -> rows are in chronological order within it.

    lag(close, 1) -> the close price from the previous trading day.
    rowsBetween(-29, 0) -> a 30-row rolling frame (this row + 29 before it).

    We compute:
      daily_return  : % change from previous close
      moving_avg_30 : 30-day rolling average close (momentum signal)
      moving_avg_60 : 60-day rolling average close (longer trend)
      volatility_30 : stddev of daily_return over 30 days (risk measure)
      week52_high   : highest high over ~252 trading days (~1 year)
      week52_low    : lowest low over ~252 trading days
    """
    win      = Window.partitionBy("symbol").orderBy("trade_date")
    win_30d  = win.rowsBetween(-29, 0)
    win_60d  = win.rowsBetween(-59, 0)
    win_252d = win.rowsBetween(-251, 0)

    return (
        df
        .withColumn("prev_close", F.lag("close", 1).over(win))
        .withColumn(
            "daily_return",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
                (F.col("close") - F.col("prev_close")) / F.col("prev_close")
            ).otherwise(F.lit(None).cast(T.DoubleType()))
        )
        .withColumn("moving_avg_30", F.avg("close").over(win_30d))
        .withColumn("moving_avg_60", F.avg("close").over(win_60d))
        .withColumn("volatility_30", F.stddev("daily_return").over(win_30d))
        .withColumn("week52_high",   F.max("high").over(win_252d))
        .withColumn("week52_low",    F.min("low").over(win_252d))
        .drop("prev_close")
    )


# ---------------------------------------------------------------------------
# Step 4: Read & clean metadata
# ---------------------------------------------------------------------------

def read_metadata(spark: SparkSession, path: str):
    """
    symbols_valid_meta.csv -> company-level info joined onto price rows.
    Market Category codes: Q, G, S, N -> mapped to human-readable labels.
    """
    mapping_expr = F.create_map(
        *[token for kv in MARKET_CATEGORY_MAP.items()
          for token in (F.lit(kv[0]), F.lit(kv[1]))]
    )
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(path)
        .select(
            F.trim(F.col("Symbol")).alias("symbol"),
            F.trim(F.col("Security Name")).alias("security_name"),
            F.trim(F.col("Listing Exchange")).alias("listing_exchange"),
            F.trim(F.col("Market Category")).alias("market_category_code"),
            F.trim(F.col("ETF")).alias("etf_flag"),
        )
        .withColumn(
            "market_category",
            F.coalesce(
                mapping_expr[F.col("market_category_code")],
                F.lit("Other")
            )
        )
        .dropDuplicates(["symbol"])
        .filter(F.col("symbol").isNotNull() & (F.col("symbol") != ""))
    )


# ---------------------------------------------------------------------------
# Step 5: Join prices + metadata (left join - keep all price rows)
# ---------------------------------------------------------------------------

def enrich(prices, meta):
    return prices.join(meta, on="symbol", how="left")


# ---------------------------------------------------------------------------
# Step 6: Write Parquet partitioned by year
# ---------------------------------------------------------------------------

def write_parquet(df, output_path: str) -> None:
    """
    Concept: Partitioned Parquet
    ----------------------------
    Partitioning by year creates one folder per year:
        output_path/year=1980/part-0000.parquet
        output_path/year=1981/part-0000.parquet  ...

    BigQuery + Spark skip entire year folders when you filter by date range.

    coalesce(8): collapse into 8 output files per year to avoid the
    "small files problem" (thousands of tiny part files is bad for perf).
    """
    (
        df
        .withColumn("year", F.year("trade_date"))
        .coalesce(8)
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(output_path)
    )
    print(f"Written to: {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(mode: str, sample_n: int = 0) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  Stock Market Pipeline - Spark Processing")
    print(f"  Mode: {mode.upper()}"
          + (f"  |  Sample: {sample_n} tickers" if sample_n else ""))
    print(f"{'='*60}\n")

    if mode == "local":
        stocks_glob = f"{LOCAL_DATA}/stocks/*.csv"
        etfs_glob   = f"{LOCAL_DATA}/etfs/*.csv"
        meta_path   = f"{LOCAL_DATA}/symbols_valid_meta.csv"
        output_path = f"{LOCAL_DATA}/processed/stock_prices.parquet"
    else:
        stocks_glob = f"{RAW_BASE}/stocks/*.csv"
        etfs_glob   = f"{RAW_BASE}/etfs/*.csv"
        meta_path   = f"{RAW_BASE}/symbols_valid_meta.csv"
        output_path = f"{PROC_BASE}/stock_prices.parquet"
        sample_n    = 0

    # 1. Read
    print(f"Reading stock CSVs{f' (first {sample_n})' if sample_n else ''}...")
    stocks = read_price_csvs(spark, stocks_glob, sample_n)

    print(f"Reading ETF CSVs{f' (first {sample_n})' if sample_n else ''}...")
    etfs   = read_price_csvs(spark, etfs_glob, sample_n)

    # 2. Combine + clean
    print("Combining and cleaning...")
    prices_clean = clean(stocks.unionByName(etfs))

    # 3. Window metrics
    print("Computing window metrics (daily_return, MA30/60, volatility, 52w)...")
    prices_enriched = add_metrics(prices_clean)

    # 4. Metadata
    print("Reading company metadata...")
    meta = read_metadata(spark, meta_path)

    # 5. Enrich
    print("Joining with metadata...")
    final = enrich(prices_enriched, meta)

    # 6. Sanity check
    total_rows       = final.count()
    distinct_symbols = final.select("symbol").distinct().count()
    date_range       = final.agg(F.min("trade_date"), F.max("trade_date")).collect()[0]

    print(f"\n{'─'*50}")
    print(f"  Rows            : {total_rows:,}")
    print(f"  Distinct symbols: {distinct_symbols:,}")
    print(f"  Date range      : {date_range[0]}  ->  {date_range[1]}")
    print(f"{'─'*50}")

    print("\nSample output (5 rows):")
    final.select(
        "trade_date", "symbol", "close", "daily_return",
        "moving_avg_30", "volatility_30", "week52_high",
        "security_name", "market_category"
    ).orderBy("symbol", "trade_date").show(5, truncate=False)

    # 7. Write
    print(f"\nWriting Parquet to:\n  {output_path}\n")
    write_parquet(final, output_path)

    print("Done!")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Market Spark Processing")
    parser.add_argument(
        "--mode",
        choices=["local", "gcs"],
        default="local",
        help="'local' reads/writes local disk; 'gcs' reads/writes GCS",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=0,
        help="(local only) Limit to first N stock/ETF files. 0 = all.",
    )
    args = parser.parse_args()
    main(args.mode, args.sample)
