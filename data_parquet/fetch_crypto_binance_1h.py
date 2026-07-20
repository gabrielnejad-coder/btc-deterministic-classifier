import time
from pathlib import Path

import ccxt
import pandas as pd


def fetch_ohlcv_binance(symbol: str, since_ts_utc: str, timeframe: str = "1h", limit: int = 1000) -> pd.DataFrame:
    ex = ccxt.binance({"enableRateLimit": True})
    since_ms = int(pd.Timestamp(since_ts_utc, tz="UTC").timestamp() * 1000)

    rows = []
    while True:
        batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=limit)
        if not batch:
            break
        rows.extend(batch)
        last = batch[-1][0]
        next_since = last + 1
        if next_since <= since_ms:
            break
        since_ms = next_since
        time.sleep(ex.rateLimit / 1000)

    df = pd.DataFrame(rows, columns=["ts_ms", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df = df.drop(columns=["ts_ms"])
    df = df[["ts", "open", "high", "low", "close", "volume"]]
    df = df.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    return df


def main():
    Path("data_parquet").mkdir(exist_ok=True)

    start = "2022-03-23 00:00:00"

    mapping = {
        "ETH": "ETH/USDT",
        "SOL": "SOL/USDT",
    }

    for asset, sym in mapping.items():
        print("FETCH", asset, sym)
        df = fetch_ohlcv_binance(sym, start, timeframe="1h")
        out = Path(f"data_parquet/{asset}_1h.parquet")
        df.to_parquet(out, index=False)
        print("WROTE", out, "rows", len(df), "ts_min", df["ts"].min(), "ts_max", df["ts"].max())


if __name__ == "__main__":
    main()
