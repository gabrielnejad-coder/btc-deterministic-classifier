"""
Fetch BTC and ETH funding rates from Binance public data repo.
No API key, no geo-restriction.
"""
from __future__ import annotations

import io
import time
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pandas as pd
import requests


DATA_DIR = Path("data_parquet")
START_YEAR = 2022
START_MONTH = 1


def _months_range():
    now = datetime.now(timezone.utc)
    dt = datetime(START_YEAR, START_MONTH, 1, tzinfo=timezone.utc)
    while dt <= now:
        yield dt.year, dt.month
        dt += relativedelta(months=1)


def fetch_funding_binance_public(symbol: str) -> pd.DataFrame:
    base = "https://data.binance.vision/data/futures/um/monthly/fundingRate"
    all_dfs = []

    print(f"  Fetching funding rates for {symbol} from data.binance.vision...")

    for year, month in _months_range():
        fname = f"{symbol}-fundingRate-{year}-{month:02d}.zip"
        url = f"{base}/{symbol}/{fname}"

        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                if not csv_names:
                    continue
                with zf.open(csv_names[0]) as f:
                    df = pd.read_csv(f)
                    all_dfs.append(df)
                    print(f"    {year}-{month:02d}: {len(df)} rows")

            time.sleep(0.1)
        except Exception as e:
            print(f"    skip {fname}: {e}")
            continue

    if not all_dfs:
        print(f"    WARNING: no funding data for {symbol}")
        return pd.DataFrame()

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"    raw columns: {list(df.columns)}")

    # Parse timestamp
    for col in ["calc_time", "fundingTime"]:
        if col in df.columns:
            df["ts"] = pd.to_datetime(df[col], unit="ms", utc=True)
            break
    if "ts" not in df.columns:
        for col in df.columns:
            if "time" in col.lower():
                try:
                    df["ts"] = pd.to_datetime(df[col], unit="ms", utc=True)
                    break
                except Exception:
                    continue

    if "ts" not in df.columns:
        print(f"    WARNING: could not parse timestamps")
        return pd.DataFrame()

    # Parse funding rate
    rate_col = None
    for col in df.columns:
        cl = col.lower()
        if "funding" in cl and "rate" in cl and "time" not in cl:
            rate_col = col
            break
        if col == "lastFundingRate":
            rate_col = col
            break
    if rate_col is None:
        print(f"    WARNING: no rate column found")
        return pd.DataFrame()

    df["funding_rate"] = pd.to_numeric(df[rate_col], errors="coerce")
    df = df[["ts", "funding_rate"]].dropna()
    df = df.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)

    print(f"    TOTAL: {len(df)} rows  {df['ts'].min()} to {df['ts'].max()}")
    return df


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for symbol, prefix in [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH")]:
        fr = fetch_funding_binance_public(symbol)
        if not fr.empty:
            path = DATA_DIR / f"{prefix}_funding_8h.parquet"
            fr.to_parquet(path, index=False)
            print(f"    WROTE {path}")
        time.sleep(0.5)

    print("\nDONE. Funding rates saved.")
    print("OI data requires separate sourcing (Binance API is geo-blocked from US).")
    print("Next: validate + canonicalize funding to 1h grid.")


if __name__ == "__main__":
    main()
