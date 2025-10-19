import requests
import pandas as pd
from datetime import datetime, timezone
from config import BRONZE, Sources

OUT_DIR = BRONZE / "crypto_price"
OUT_DIR.mkdir(parents=True, exist_ok=True)

resp = requests.get(Sources().crypto_simple, timeout=30)
resp.raise_for_status()
js = resp.json()

rows = []
now = datetime.now(timezone.utc)
for asset in ("bitcoin", "ethereum"):
    for quote in ("eur", "usd"):
        price = js.get(asset, {}).get(quote)
        if price is not None:
            rows.append(
                {
                    "timestamp_utc": now.isoformat(),
                    "symbol": "BTC" if asset == "bitcoin" else "ETH",
                    "quote_ccy": quote.upper(),
                    "price": float(price),
                    "source": "coingecko",
                }
            )

df = pd.DataFrame(rows)
part = OUT_DIR / now.strftime("year=%Y/month=%m/day=%d") / "part.parquet"
part.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(part, index=False)
print(f"Wrote crypto prices to {part}")
