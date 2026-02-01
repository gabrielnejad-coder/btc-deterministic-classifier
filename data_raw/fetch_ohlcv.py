import time
from datetime import datetime, timezone
import pandas as pd
import ccxt
import yaml

RAW_PATH = "data_raw/BTCUSD_USD_1h_raw.parquet"
HOUR_MS = 60 * 60 * 1000

def utc_ms(dt_str: str) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def main():
    cfg = yaml.safe_load(open("config/recent_analyze.yaml"))
    exchange_id = cfg["exchange"]
    symbol = cfg["symbol"]
    timeframe = cfg["timeframe"]
    start = utc_ms(cfg["start_date"])

    ex = getattr(ccxt, exchange_id)({"enableRateLimit": True})
    ex.load_markets()

    print("exchange", exchange_id)
    print("symbol", symbol)
    print("timeframe", timeframe)
    print("start_utc", cfg["start_date"])
    print("raw_path", RAW_PATH)

    limit = 720

    newest = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=None, limit=limit)
    if not newest:
        print("no_data_returned_from_since_none")
        df = pd.DataFrame(columns=["ts_ms","open","high","low","close","volume"])
        df.to_parquet(RAW_PATH, index=False)
        return

    all_rows = newest[:]
    earliest_ts = all_rows[0][0]
    loops = 1
    print("seed_rows", len(newest), "earliest_seed", ms_to_iso(earliest_ts), "latest_seed", ms_to_iso(all_rows[-1][0]))

    while earliest_ts > start:
        loops += 1
        target_since = max(start, earliest_ts - (limit * HOUR_MS))
        batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=target_since, limit=limit)

        if not batch:
            print("empty_batch_at", target_since, ms_to_iso(target_since))
            break

        batch = [r for r in batch if r[0] < earliest_ts]
        if not batch:
            print("no_older_rows_returned_at", target_since, ms_to_iso(target_since))
            break

        all_rows = batch + all_rows
        earliest_ts = all_rows[0][0]

        if loops % 10 == 0:
            print("loops", loops, "rows", len(all_rows), "earliest", ms_to_iso(earliest_ts), "latest", ms_to_iso(all_rows[-1][0]))

        time.sleep(ex.rateLimit / 1000)

    df = pd.DataFrame(all_rows, columns=["ts_ms", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["ts_ms"]).sort_values("ts_ms").reset_index(drop=True)
    df.to_parquet(RAW_PATH, index=False)

    print("done_rows", len(df))
    if len(df):
        print("done_first", ms_to_iso(int(df["ts_ms"].iloc[0])))
        print("done_last", ms_to_iso(int(df["ts_ms"].iloc[-1])))

if __name__ == "__main__":
    main()
