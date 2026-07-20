import pandas as pd

from features.build_features import build_features
from features.schema import FeatureObject
from model.classifier import classify
from model.signal_filters import apply_signal_filters


def build_signals_v2(
    df: pd.DataFrame,
    confirm_bars: int = 3,
    hold_bars: int = 72,
    min_abs_ret1: float = 0.0005,
    max_vol24: float = 0.08,
    trade_threshold: float = 0.55,
    flip_threshold: float = 0.60,
) -> pd.Series:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    if df["ts"].duplicated().any():
        raise ValueError("df['ts'] has duplicates")

    feats = build_features(df).copy()
    feats["ts"] = pd.to_datetime(feats["ts"], utc=True)
    feats = feats.sort_values("ts").reset_index(drop=True)

    ts_index = pd.DatetimeIndex(df["ts"], name="ts")
    sig = pd.Series("flat", index=ts_index, dtype="object")

    state = "flat"

    for row in feats.itertuples(index=False):
        ts_utc = pd.Timestamp(row.ts)
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.tz_localize("UTC")
        else:
            ts_utc = ts_utc.tz_convert("UTC")

        r1 = float(row.ret_1)
        v24 = float(row.vol_24)

        if abs(r1) < float(min_abs_ret1):
            sig.at[ts_utc] = state
            continue

        if v24 > float(max_vol24):
            sig.at[ts_utc] = state
            continue

        fobj = FeatureObject(
            ts=str(ts_utc),
            close=float(row.close),

            ret_1=float(row.ret_1),
            ret_2=float(row.ret_2),
            ret_4=float(row.ret_4),
            ret_8=float(row.ret_8),
            ret_12=float(row.ret_12),
            ret_24=float(row.ret_24),

            mom_12=float(row.mom_12),

            vol_12=float(row.vol_12),
            vol_24=float(row.vol_24),
            vol_48=float(row.vol_48),

            range_1=float(row.range_1),

            vol_chg_1=float(row.vol_chg_1),
            vol_z_48=float(row.vol_z_48),
        )

        out = classify(fobj)
        p_up = float(out["p_up"])
        p_down = float(out["p_down"])
        p_flat = float(out["p_flat"])

        best_side = "up" if p_up >= p_down else "down"
        best_p = p_up if best_side == "up" else p_down

        if state == "flat":
            if best_p >= float(trade_threshold) and best_p >= p_flat:
                state = best_side
            else:
                state = "flat"
        elif state == "up":
            if p_down >= float(flip_threshold) and p_down >= p_flat:
                state = "down"
            else:
                state = "up"
        elif state == "down":
            if p_up >= float(flip_threshold) and p_up >= p_flat:
                state = "up"
            else:
                state = "down"

        sig.at[ts_utc] = state

    return apply_signal_filters(sig, confirm_bars=confirm_bars, hold_bars=hold_bars)
