from pathlib import Path
import duckdb

def test_bronze_exists():
    assert Path("data/bronze/fx_rate").exists()
    assert Path("data/bronze/crypto_price").exists()

def test_silver_not_empty():
    con = duckdb.connect("warehouse.duckdb")
    n_fx = con.execute("SELECT COUNT(*) FROM silver_fx").fetchone()[0]
    n_crypto = con.execute("SELECT COUNT(*) FROM silver_crypto").fetchone()[0]
    assert n_fx > 0
    assert n_crypto > 0
