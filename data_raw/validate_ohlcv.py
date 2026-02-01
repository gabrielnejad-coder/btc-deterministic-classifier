import json
from datetime import datetime, timezone
import pandas as pd

RAW_PATH = "data_raw/BTCUSD_USD_1h_raw.parquet"
REPORT_PATH = "reports/data_quality.json"
HOUR_MS = 60 * 60 * 1000

def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def main():
    df = pd.read_parquet(RAW_PATH)
    issues = {}

    df = df.drop_duplicates(subset=["ts_ms"]).sort_values("ts_ms").reset_index(drop=True)

    issues["rows"] = int(len(df))
    issues["first_utc"] = ms_to_iso(int(df["ts_ms"].iloc[0]))
    issues["last_utc"] = ms_to_iso(int(df["ts_ms"].iloc[-1]))

    dup_count = int(df["ts_ms"].duplicated().sum())
    issues["duplicate_count"] = dup_count

    diffs = df["ts_ms"].diff().dropna()
    issues["non_hour_step_count"] = int((diffs != HOUR_MS).sum())

    missing_hours = 0
    for i in range(1, len(df)):
        gap = int(df.loc[i, "ts_ms"] - df.loc[i - 1, "ts_ms"])
        if gap > HOUR_MS:
            missing_hours += int(gap / HOUR_MS) - 1

    issues["missing_hours_total"] = int(missing_hours)
    issues["negative_price_rows"] = int(((df[["open","high","low","close"]] <= 0).any(axis=1)).sum())
    issues["negative_volume_rows"] = int((df["volume"] < 0).sum())
    issues["zero_close_rows"] = int((df["close"] == 0).sum())

    issues["pass"] = bool(
        issues["duplicate_count"] == 0 and
        issues["missing_hours_total"] == 0 and
        issues["negative_price_rows"] == 0 and
        issues["negative_volume_rows"] == 0 and
        issues["zero_close_rows"] == 0
    )

    with open(REPORT_PATH, "w") as f:
        json.dump(issues, f, indent=2)

    print(json.dumps(issues, indent=2))

if __name__ == "__main__":
    main()
