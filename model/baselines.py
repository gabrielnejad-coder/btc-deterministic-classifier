import pandas as pd

def _ts_utc(df: pd.DataFrame) -> pd.DatetimeIndex:
    ts = pd.to_datetime(df["ts"], utc=True)  # Simple, handles all cases
    return pd.DatetimeIndex(ts, name="ts")

def always_up(df: pd.DataFrame) -> pd.Series:
    ts = _ts_utc(df)
    return pd.Series("up", index=ts, dtype="object")

def yesterday_equals_today(df: pd.DataFrame) -> pd.Series:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    idx = pd.DatetimeIndex(df["ts"], name="ts")
    return pd.Series("flat", index=idx, dtype="object")
