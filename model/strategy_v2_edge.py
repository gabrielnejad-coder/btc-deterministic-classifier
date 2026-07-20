import pandas as pd

from features.build_features import build_features
from features.schema import FeatureObject
from model.classifier import classify
from model.signal_filters import apply_signal_filters


def build_signals_v2_edge(
    df: pd.DataFrame,
    confirm_bars: int = 3,
    hold_bars: int = 72,
    min_abs_ret1: float = 0.0005,
    max_vol24: float = 0.08,
    edge_threshold: float = 0.04,
    flip_edge_threshold: float = 0.06,
) -> pd.Series:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    feats = build_features(df).copy()
    feats["ts"] = pd.to_datetime(feats["ts"], utc=True)
    feats = feats.sort_values("ts").reset_index(drop=True)

    idx = pd.DatetimeIndex(df["ts"], name="ts")
    sig = pd.Series("flat", index=idx, dtype="object")

    state = "flat"

    for r in feats.itertuples(index=False):
        ts_utc = pd.Timestamp(r.ts)
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.tz_localize("UTC")
        else:
            ts_utc = ts_utc.tz_convert("UTC")

        if abs(float(r.ret_1)) < float(min_abs_ret1):
            sig.at[ts_utc] = state
            continue

        if float(r.vol_24) > float(max_vol24):
            sig.at[ts_utc] = state
            continue

        fobj = FeatureObject(
            ts=str(ts_utc),
            close=float(r.close),

            ret_1=float(r.ret_1),
            ret_2=float(r.ret_2),
            ret_4=float(r.ret_4),
            ret_8=float(r.ret_8),
            ret_12=float(r.ret_12),
            ret_24=float(r.ret_24),

            mom_12=float(r.mom_12),

            vol_12=float(r.vol_12),
            vol_24=float(r.vol_24),
            vol_48=float(r.vol_48),

            range_1=float(r.range_1),

            vol_chg_1=float(r.vol_chg_1),
            vol_z_48=float(r.vol_z_48),
        )

        out = classify(fobj)
        probs = {
            "up": float(out["p_up"]),
            "down": float(out["p_down"]),
            "flat": float(out["p_flat"]),
        }

        ordered = sorted(probs.items(), key=lambda kv: kv[1], reverse=True)
        best_side, best_p = ordered[0]
        second_p = ordered[1][1]
        edge = best_p - second_p

        if state == "flat":
            if best_side in ("up", "down") and edge >= edge_threshold:
                state = best_side
        elif state == "up":
            if best_side == "down" and edge >= flip_edge_threshold:
                state = "down"
        elif state == "down":
            if best_side == "up" and edge >= flip_edge_threshold:
                state = "up"

        sig.at[ts_utc] = state

    return apply_signal_filters(sig, confirm_bars=confirm_bars, hold_bars=hold_bars)
