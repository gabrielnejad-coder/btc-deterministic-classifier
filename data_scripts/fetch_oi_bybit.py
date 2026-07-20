"""
Fetch BTC and ETH Open Interest from Bybit public API.
Not geo-blocked from US. Free, no API key.
1h interval, paginated back to 2022.
"""
from __future__ import annotations

import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests


DATA_DIR = Path("data_parquet")
BASE_URL = "https://api.bybit.com/v5/market/open-interest"
START_TS = int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def fetch_oi(symbol: str, prefix: str) -> pd.DataFrame:
    all_rows = []
    cursor = ""
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = START_TS

    print(f"  Fetching OI for {symbol}...")

    # Bybit returns newest first, use cursor for pagination
    # We'll fetch in reverse then sort
    page = 0
    while True:
        params = {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "1h",
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor
        else:
            params["startTime"] = start_ms
            params["endTime"] = end_ms

        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            data = resp.json()

            if data.get("retCode") != 0:
                print(f"    Error: {data.get('retMsg', 'unknown')}")
                break

            rows = data.get("result", {}).get("list", [])
            if not rows:
                break

            all_rows.extend(rows)
            page += 1

            if page % 50 == 0:
                print(f"    page {page}: {len(all_rows)} rows so far...")

            # Get next cursor
            next_cursor = data.get("result", {}).get("nextPageCursor", "")
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

            time.sleep(0.12)

        except Exception as e:
            print(f"    Error on page {page}: {e}")
            break

    if not all_rows:
        print(f"    No data retrieved")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    print(f"    Raw columns: {list(df.columns)}")
    print(f"    Raw rows: {len(df)}")

    # Parse
    df["ts"] = pd.to_datetime(df["timestamp"].astype(int), unit="ms", utc=True)
    df["oi_value"] = pd.to_numeric(df["openInterest"], errors="coerce")
    df = df[["ts", "oi_value"]].dropna()
    df = df.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)

    print(f"    Clean rows: {len(df)}")
    print(f"    Range: {df['ts'].min()} to {df['ts'].max()}")

    return df


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for symbol, prefix in [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")]:
        df = fetch_oi(symbol, prefix)
        if not df.empty:
            path = DATA_DIR / f"{prefix}_oi_bybit_1h.parquet"
            df.to_parquet(path, index=False)
            print(f"    WROTE {path}")

    print("\nDONE.")


if __name__ == "__main__":
    main()
