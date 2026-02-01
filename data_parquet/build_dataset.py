import json
from datetime import datetime, timezone
import pandas as pd
import yaml

RAW_PATH = "data_raw/BTCUSD_USD_1h_raw.parquet"
CANON_PATH = "data_parquet/BTCUSD_USD_1h_20220323_now.parquet"
META_PATH = "reports/dataset_meta.json"

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def main():
    cfg = yaml.safe_load(open("config/v1.yaml"))

    df = pd.read_parquet(RAW_PATH)
    df = df.drop_duplicates(subset=["ts_ms"]).sort_values("ts_ms").reset_index(drop=True)

    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    df = df.drop(columns=["ts_ms"])
    df = df[["ts", "open", "high", "low", "close", "volume"]]

    df.to_parquet(CANON_PATH, index=False)

    meta = {
        "exchange": cfg.get("exchange"),
        "symbol": cfg.get("symbol"),
        "timeframe": cfg.get("timeframe"),
        "start_date_config": cfg.get("start_date"),
        "rows": int(len(df)),
        "first_ts": str(df["ts"].iloc[0]),
        "last_ts": str(df["ts"].iloc[-1]),
        "build_time_utc": now_utc_iso(),
        "canonical_path": CANON_PATH,
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)

    print("canonical_rows", meta["rows"])
    print("first_ts", meta["first_ts"])
    print("last_ts", meta["last_ts"])
    print("canonical_path", CANON_PATH)
    print("meta_path", META_PATH)

if __name__ == "__main__":
    main()
