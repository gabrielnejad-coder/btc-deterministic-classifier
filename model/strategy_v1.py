import pandas as pd

from features.build_features import build_features
from model.signal_filters import apply_signal_filters


def build_signals_v1(
    df: pd.DataFrame,
    confirm_bars: int = 3,
    hold_bars: int = 72,
) -> pd.Series:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    feats = build_features(df)
    feats = feats.dropna(subset=["ts", "ret_4"]).reset_index(drop=True)

    ts_index = pd.DatetimeIndex(df["ts"], name="ts")
    sig = pd.Series("flat", index=ts_index, dtype="object")

    state = "flat"

    for row in feats.itertuples(index=False):
        ts_utc = pd.Timestamp(row.ts)
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.tz_localize("UTC")
        else:
            ts_utc = ts_utc.tz_convert("UTC")

        r4 = float(row.ret_4)

        state = "up" if r4 > 0 else "down"
        sig.at[ts_utc] = state

    return apply_signal_filters(sig, confirm_bars=confirm_bars, hold_bars=hold_bars)
