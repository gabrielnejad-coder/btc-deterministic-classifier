import pandas as pd


def always_up(ts: pd.DatetimeIndex) -> pd.Series:
    return pd.Series("up", index=ts, dtype="object")


def yesterday_equals_today(close: pd.Series) -> pd.Series:
    close = close.copy()
    close.index = pd.to_datetime(close.index, utc=True)

    ret = close.pct_change()
    sig = pd.Series(index=close.index, dtype="object")
    sig[ret > 0] = "up"
    sig[ret < 0] = "down"
    sig[ret == 0] = "up"
    sig = sig.fillna("up")
    return sig
