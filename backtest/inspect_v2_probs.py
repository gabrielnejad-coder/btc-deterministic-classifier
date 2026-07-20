import pandas as pd
import numpy as np

from backtest.walkforward import split_walkforward
from features.build_features import build_features
from features.schema import FeatureObject
from model.classifier import classify


def main():
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(df)
    vdf = splits["validate"].copy().reset_index(drop=True)

    feats = build_features(vdf).copy()
    feats["ts"] = pd.to_datetime(feats["ts"], utc=True)
    feats = feats.sort_values("ts").reset_index(drop=True)

    rows = []
    for r in feats.itertuples(index=False):
        ts_utc = pd.Timestamp(r.ts)
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.tz_localize("UTC")
        else:
            ts_utc = ts_utc.tz_convert("UTC")

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
        p_up = float(out["p_up"])
        p_down = float(out["p_down"])
        p_flat = float(out["p_flat"])
        best_p = max(p_up, p_down)

        rows.append((p_up, p_down, p_flat, best_p))

    a = np.array(rows, dtype=float)
    p_up = a[:, 0]
    p_down = a[:, 1]
    p_flat = a[:, 2]
    best_p = a[:, 3]

    def q(x):
        return {
            "min": float(np.min(x)),
            "p10": float(np.quantile(x, 0.10)),
            "p50": float(np.quantile(x, 0.50)),
            "p90": float(np.quantile(x, 0.90)),
            "max": float(np.max(x)),
        }

    print("ROWS", int(len(best_p)))
    print("Q_best_p", q(best_p))
    print("Q_p_flat", q(p_flat))

    print("PCT_best_ge_0.55", float(np.mean(best_p >= 0.55)))
    print("PCT_best_ge_0.60", float(np.mean(best_p >= 0.60)))
    print("PCT_best_ge_0.65", float(np.mean(best_p >= 0.65)))
    print("PCT_best_ge_0.70", float(np.mean(best_p >= 0.70)))

    print("PCT_pflat_is_max", float(np.mean(p_flat >= np.maximum(p_up, p_down))))


if __name__ == "__main__":
    main()
