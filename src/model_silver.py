import duckdb
from config import BRONZE, SILVER
from pathlib import Path

SILVER.mkdir(parents=True, exist_ok=True)
con = duckdb.connect(database=(Path("warehouse.duckdb").as_posix()))

con.execute(
    f"""
    CREATE OR REPLACE TABLE silver_fx AS
    SELECT
      CAST(date AS DATE) AS date,
      'EUR' AS base_ccy,
      CAST(quote_ccy AS VARCHAR) AS quote_ccy,
      CAST(rate AS DOUBLE) AS rate,
      'ECB' AS source
    FROM read_parquet('{(BRONZE / 'fx_rate' / '**' / '*.parquet').as_posix()}');

    CREATE OR REPLACE TABLE silver_crypto AS
    SELECT
      CAST(timestamp_utc AS TIMESTAMP) AS ts,
      CAST(symbol AS VARCHAR) AS symbol,
      CAST(quote_ccy AS VARCHAR) AS quote_ccy,
      CAST(price AS DOUBLE) AS price,
      CAST(source AS VARCHAR) AS source
    FROM read_parquet('{(BRONZE / 'crypto_price' / '**' / '*.parquet').as_posix()}');
    """
)
print("Built silver tables in DuckDB")
