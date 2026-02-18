import pandas as pd

def _ts_utc(df: pd.DataFrame) -> pd.DatetimeIndex:
    ts = pd.to_datetime(df["ts"], utc=True)  # Simple, handles all cases
    return pd.DatetimeIndex(ts, name="ts")

def always_up(df: pd.DataFrame) -> pd.Series:
    ts = _ts_utc(df)
    return pd.Series("up", index=ts, dtype="object")

def yesterday_equals_today(df: pd.DataFrame) -> pd.Series:
    ts = _ts_utc(df)
    close = pd.to_numeric(df["close"], errors="coerce")
    close.index = ts  # FIX: Align index before pct_change
    ret = close.pct_change()
    sig = pd.Series("up", index=ts, dtype="object")
    sig.loc[ret < 0] = "down"
    return sig.fillna("up")
