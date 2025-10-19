from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
BRONZE = DATA_DIR / "bronze"
SILVER = DATA_DIR / "silver"
GOLD = DATA_DIR / "gold"

for p in (BRONZE, SILVER, GOLD):
    p.mkdir(parents=True, exist_ok=True)

@dataclass(frozen=True)
class Sources:
    ecb_csv: str = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.csv"
    crypto_simple: str = (
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=eur,usd"
    )

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
