import duckdb
con = duckdb.connect("warehouse.duckdb")

fx = con.execute("""
SELECT * FROM gold_fx_daily
WHERE quote_ccy='USD'
ORDER BY date DESC
LIMIT 7
""").fetchdf()

crypto = con.execute("""
SELECT * FROM gold_crypto_daily
WHERE quote_ccy='EUR'
ORDER BY date DESC
LIMIT 14
""").fetchdf()

aligned = con.execute("""
SELECT * FROM gold_crypto_fx_aligned
WHERE quote_ccy='USD'
ORDER BY date DESC
LIMIT 14
""").fetchdf()

fx.to_csv("data/gold/sample_fx_usd_last7.csv", index=False)
crypto.to_csv("data/gold/sample_crypto_eur_last14.csv", index=False)
aligned.to_csv("data/gold/sample_crypto_usd_aligned.csv", index=False)
print("Wrote sample CSVs in data/gold/")
