from prefect import flow, task
import subprocess

@task
def ingest_fx():
    subprocess.run(["python", "src/ingest_ecb.py"], check=True)

@task
def ingest_crypto():
    subprocess.run(["python", "src/ingest_crypto.py"], check=True)

@task
def build_silver():
    subprocess.run(["python", "src/model_silver.py"], check=True)

@task
def build_gold():
    import duckdb
    con = duckdb.connect("warehouse.duckdb")
    with open("src/model_gold.sql", "r", encoding="utf-8") as f:
        con.execute(f.read())
    con.close()

@flow(name="daily_crypto_fx")
def daily():
    ingest_fx()
    ingest_crypto()
    build_silver()
    build_gold()

if __name__ == "__main__":
    daily()

