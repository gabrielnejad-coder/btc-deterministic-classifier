import pandas as pd

from features.build_features import build_features
from features.schema import FeatureObject
from model.classifier import classify
from model.signal_filters import apply_signal_filters


def _to_utc_ts(x) -> pd.Timestamp:
    ts = pd.Timestamp(x)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def build_signals_v1(
    df: pd.DataFrame,
    confirm_bars: int = 2,
    hold_bars: int = 24,
) -> pd.Series:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts")

    if df["ts"].duplicated().any():
        raise ValueError("df['ts'] has duplicates, engine alignment will be unreliable")

    ts_index = pd.DatetimeIndex(df["ts"], name="ts")

    sig = pd.Series("flat", index=ts_index, dtype="object")

    feats = build_features(df)

    required = ["ts", "close", "ret_1", "ret_4", "ret_24", "vol_24"]
    missing = [c for c in required if c not in feats.columns]
    if missing:
        raise KeyError(f"build_features missing required columns: {missing}")

    feats = feats.dropna(subset=required)

    for row in feats.itertuples(index=False):
        ts_utc = _to_utc_ts(row.ts)

        if ts_utc not in sig.index:
            continue

        fobj = FeatureObject(
            ts=ts_utc.isoformat(),
            close=float(row.close),
            ret_1=float(row.ret_1),
            ret_4=float(row.ret_4),
            ret_24=float(row.ret_24),
            vol_24=float(row.vol_24),
        )

        out = classify(fobj)
        direction = out.get("direction")

        if direction == "up":
            sig.at[ts_utc] = "up"
        elif direction == "down":
            sig.at[ts_utc] = "down"
        else:
            raise ValueError(f"Classifier returned invalid direction: {direction}")

    sig = apply_signal_filters(sig, confirm_bars=confirm_bars, hold_bars=hold_bars)

    valid = {"up", "down", "flat"}
    bad = set(pd.unique(sig.dropna())) - valid
    if bad:
        raise ValueError(f"Invalid signals after filters: {bad}")

    return sig
