import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.walkforward import split_walkforward
from features.build_features import build_features

HORIZON = 12
CANDIDATES = ["ETH", "SOL", "SPY", "QQQ", "DXY", "VIX", "MSTR", "NVDA"]

def fwd_ret(close: pd.Series) -> pd.Series:
    return close.shift(-HORIZON) / close - 1.0

def ic(x: pd.Series, y: pd.Series) -> float:
    m = x.notna() & y.notna()
    if m.sum() < 500:
        return float("nan")
    return float(np.corrcoef(x[m], y[m])[0, 1])

def rolling_ic_std(x: pd.Series, y: pd.Series, window: int = 500) -> float:
    vals = []
    for i in range(window, len(x)):
        v = ic(x.iloc[i-window:i], y.iloc[i-window:i])
        if not np.isnan(v):
            vals.append(v)
    return float(np.std(vals)) if vals else float("nan")

def load_asset(asset: str) -> pd.DataFrame:
    path = Path(f"data_parquet/{asset}_1h.parquet")
    if not path.exists():
        raise FileNotFoundError(str(path))
    df = pd.read_parquet(path).copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)
    return df

def score_candidate(train_btc: pd.DataFrame, asset: str) -> dict:
    try:
        df = load_asset(asset)
    except FileNotFoundError:
        return {"asset": asset, "error": "missing_data"}

    feats = build_features(df).copy()
    feats["ts"] = pd.to_datetime(feats["ts"], utc=True)

    feats = feats.rename(columns={c: f"{asset}_{c}" for c in feats.columns if c != "ts"})

    merged = train_btc.merge(feats, on="ts", how="inner")

    if len(merged) < 2000:
        return {"asset": asset, "error": f"low_overlap_rows:{len(merged)}"}

    y = merged["BTC_fwd_ret_12h"]

    feature_scores = {}
    for col in merged.columns:
        if col.startswith(asset + "_"):
            x = merged[col]
            feature_scores[col] = {
                "ic_0": ic(x, y),
                "ic_1": ic(x.shift(1), y),
                "ic_2": ic(x.shift(2), y),
                "ic_4": ic(x.shift(4), y),
                "stability_std": rolling_ic_std(x, y),
                "n": int((x.notna() & y.notna()).sum()),
            }

    return {"asset": asset, "feature_scores": feature_scores, "rows": int(len(merged))}

def main():
    Path("reports").mkdir(exist_ok=True)

    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(btc)
    train = splits["train"].copy().reset_index(drop=True)

    btc_feats = build_features(train).copy()
    btc_feats["ts"] = pd.to_datetime(btc_feats["ts"], utc=True)
    btc_feats = btc_feats.rename(columns={c: f"BTC_{c}" for c in btc_feats.columns if c != "ts"})
    btc_feats["BTC_fwd_ret_12h"] = fwd_ret(btc_feats["BTC_close"])

    results = []
    for a in CANDIDATES:
        print("Scoring", a)
        results.append(score_candidate(btc_feats[["ts", "BTC_fwd_ret_12h"]], a))

    Path("reports/cross_market_scores.json").write_text(json.dumps(results, indent=2) + "\n")
    print("WROTE reports/cross_market_scores.json")

if __name__ == "__main__":
    main()
