"""
Fetch BTC and ETH Open Interest from OKX public API.
"""
from __future__ import annotations

import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests


DATA_DIR = Path("data_parquet")
URL = "https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history"
START_TS = int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def fetch_oi(inst_id: str) -> pd.DataFrame:
    all_rows = []
    page = 0
    last_oldest = None

    print(f"  Fetching OI for {inst_id}...")

    while True:
        params = {
            "instId": inst_id,
            "period": "1H",
        }
        # OKX pagination: pass the oldest timestamp from previous batch as 'after'
        # 'after' means "return records older than this timestamp"
        if last_oldest is not None:
            params["after"] = str(last_oldest)

        try:
            resp = requests.get(URL, params=params, timeout=30)
            data = resp.json()

            if data.get("code") != "0":
                print(f"    API error: {data.get('msg')}")
                break

            rows = data.get("data", [])
            if not rows:
                break

            all_rows.extend(rows)
            page += 1

            # Find oldest timestamp in this batch
            oldest_ts = min(int(r[0]) for r in rows)

            # Stop if we've reached our start date
            if oldest_ts <= START_TS:
                break

            # Stop if not making progress
            if last_oldest is not None and oldest_ts >= last_oldest:
                break

            last_oldest = oldest_ts

            if page % 100 == 0:
                dt = datetime.fromtimestamp(oldest_ts/1000, tz=timezone.utc)
                print(f"    page {page}: {len(all_rows)} rows, back to {dt.date()}")

            time.sleep(0.12)

        except Exception as e:
            print(f"    Error page {page}: {e}")
            time.sleep(2)
            continue

    if not all_rows:
        print(f"    No data")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=["ts_ms", "oi_contracts", "oi_coin", "oi_usd"])
    df["ts"] = pd.to_datetime(df["ts_ms"].astype(int), unit="ms", utc=True)
    df["oi_contracts"] = pd.to_numeric(df["oi_contracts"], errors="coerce")
    df["oi_usd"] = pd.to_numeric(df["oi_usd"], errors="coerce")
    df = df[["ts", "oi_contracts", "oi_usd"]].dropna()
    df = df.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)

    print(f"    total rows: {len(df)}")
    print(f"    range: {df['ts'].min()} to {df['ts'].max()}")
    return df


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for inst_id, prefix in [("BTC-USDT-SWAP", "BTC"), ("ETH-USDT-SWAP", "ETH")]:
        df = fetch_oi(inst_id)
        if not df.empty:
            path = DATA_DIR / f"{prefix}_oi_okx_1h.parquet"
            df.to_parquet(path, index=False)
            print(f"    WROTE {path}")
        time.sleep(1)

    print("\nDONE.")


if __name__ == "__main__":
    main()
