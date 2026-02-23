import pandas as pd

from features.build_features import build_features
from features.schema import FeatureObject
from model.classifier import classify
from model.signal_filters import apply_signal_filters


def build_signals_v2(
    df: pd.DataFrame,
    confirm_bars: int = 3,
    hold_bars: int = 72,
    min_abs_ret1: float = 0.001,
    max_vol24: float = 0.05,
) -> pd.Series:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts")

    if df["ts"].duplicated().any():
        raise ValueError("df['ts'] has duplicates, engine alignment will be unreliable")

    feats = build_features(df)
    feats = feats.dropna(subset=["ts", "close", "ret_1", "ret_4", "ret_24", "vol_24"])

    ts_index = pd.DatetimeIndex(df["ts"], name="ts")
    sig = pd.Series("flat", index=ts_index, dtype="object")
    ts_set = set(ts_index)

    for row in feats.itertuples(index=False):
        ts_utc = pd.Timestamp(row.ts)
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.tz_localize("UTC")
        else:
            ts_utc = ts_utc.tz_convert("UTC")

        if ts_utc not in ts_set:
            continue

        r1 = float(row.ret_1)
        v24 = float(row.vol_24)

        if abs(r1) < min_abs_ret1:
            continue

        if v24 > max_vol24:
            continue

        fobj = FeatureObject(
            ts=str(ts_utc),
            close=float(row.close),
            ret_1=float(row.ret_1),
            ret_4=float(row.ret_4),
            ret_24=float(row.ret_24),
            vol_24=float(row.vol_24),
        )

        out = classify(fobj)
        direction = out.get("direction")

        if direction not in ("up", "down"):
            continue

        # FIX: invert direction (your flipped test proves current mapping is backwards)
        if direction == "up":
            direction = "down"
        else:
            direction = "up"

        sig.at[ts_utc] = direction

    return apply_signal_filters(sig, confirm_bars=confirm_bars, hold_bars=hold_bars)
