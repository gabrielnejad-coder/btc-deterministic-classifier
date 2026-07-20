from pathlib import Path
import time

import ccxt
import pandas as pd


def _utc_ms(s: str) -> int:
    return int(pd.Timestamp(s, tz="UTC").timestamp() * 1000)


def _make_exchange() -> ccxt.coinbase:
    ex = ccxt.coinbase({"enableRateLimit": True})
    ex.timeout = 30000
    return ex


def fetch_ohlcv_coinbase(symbol: str, start_utc: str, out_path: Path) -> pd.DataFrame:
    ex = _make_exchange()

    existing = None
    since_ms = _utc_ms(start_utc)

    if out_path.exists():
        existing = pd.read_parquet(out_path).copy()
        existing["ts"] = pd.to_datetime(existing["ts"], utc=True)
        existing = existing.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        if len(existing) > 0:
            last_ts = existing["ts"].max()
            since_ms = int(last_ts.timestamp() * 1000) + 1

    rows = []
    retries = 0

    while True:
        try:
            batch = ex.fetch_ohlcv(symbol, timeframe="1h", since=since_ms, limit=300)
            retries = 0
        except ccxt.RequestTimeout:
            retries += 1
            if retries > 10:
                raise
            time.sleep(1.5 * retries)
            continue
        except Exception:
            retries += 1
            if retries > 10:
                raise
            time.sleep(1.5 * retries)
            continue

        if not batch:
            break

        rows.extend(batch)
        last = batch[-1][0]
        next_since = last + 1
        if next_since <= since_ms:
            break
        since_ms = next_since

        time.sleep(0.15)

        if len(rows) >= 5000:
            tmp = pd.DataFrame(rows, columns=["ts_ms", "open", "high", "low", "close", "volume"])
            tmp["ts"] = pd.to_datetime(tmp["ts_ms"], unit="ms", utc=True)
            tmp = tmp.drop(columns=["ts_ms"])
            tmp = tmp[["ts", "open", "high", "low", "close", "volume"]]
            tmp = tmp.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)

            if existing is not None:
                combined = pd.concat([existing, tmp], ignore_index=True)
            else:
                combined = tmp

            combined = combined.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
            combined.to_parquet(out_path, index=False)

            existing = combined
            rows = []

    df = pd.DataFrame(rows, columns=["ts_ms", "open", "high", "low", "close", "volume"])
    if len(df) > 0:
        df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df = df.drop(columns=["ts_ms"])
        df = df[["ts", "open", "high", "low", "close", "volume"]]
        df = df.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)

    if existing is not None and len(df) > 0:
        out = pd.concat([existing, df], ignore_index=True)
    elif existing is not None:
        out = existing
    else:
        out = df

    out = out.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    out.to_parquet(out_path, index=False)
    return out


def main():
    Path("data_parquet").mkdir(exist_ok=True)
    start = "2022-03-23 00:00:00"

    mapping = {
        "ETH": "ETH/USD",
        "SOL": "SOL/USD",
    }

    for asset, sym in mapping.items():
        out_path = Path(f"data_parquet/{asset}_1h.parquet")
        print("FETCH", asset, sym, "->", out_path)
        df = fetch_ohlcv_coinbase(sym, start, out_path)
        print("WROTE", out_path, "rows", len(df), "ts_min", df["ts"].min(), "ts_max", df["ts"].max())


if __name__ == "__main__":
    main()
