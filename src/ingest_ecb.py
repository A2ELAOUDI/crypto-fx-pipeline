import io
import requests
import pandas as pd
from pathlib import Path
from config import BRONZE, Sources

OUT_DIR = BRONZE / "fx_rate"
OUT_DIR.mkdir(parents=True, exist_ok=True)

resp = requests.get(Sources().ecb_csv, timeout=30)
resp.raise_for_status()
raw = pd.read_csv(io.StringIO(resp.text))

raw = raw.rename(columns={raw.columns[0]: "date"})
raw["date"] = pd.to_datetime(raw["date"])

long_df = raw.melt(id_vars=["date"], var_name="quote_ccy", value_name="rate")
long_df = long_df.dropna(subset=["rate"])
long_df["base_ccy"] = "EUR"
long_df["source"] = "ECB"

long_df["year"] = long_df["date"].dt.year
long_df["month"] = long_df["date"].dt.month

for (y, m), part in long_df.groupby(["year", "month"], dropna=False):
    out = OUT_DIR / f"year={y}" / f"month={m:02d}" / "part.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    part.drop(columns=["year", "month"], errors="ignore").to_parquet(out, index=False)

print(f"Wrote ECB FX parquet to {OUT_DIR}")
