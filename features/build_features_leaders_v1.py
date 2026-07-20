import numpy as np
import pandas as pd

def _safe_pct_change(x: pd.Series, periods: int) -> pd.Series:
    r = x.pct_change(periods)
    r = r.replace([np.inf, -np.inf], np.nan)
    return r

def _add_basic_features(d: pd.DataFrame, prefix: str) -> pd.DataFrame:
    d = d.sort_values("ts").reset_index(drop=True)

    close = pd.to_numeric(d["close"], errors="coerce")
    vol = pd.to_numeric(d["volume"], errors="coerce").fillna(0.0)

    d[f"{prefix}_ret_1"] = _safe_pct_change(close, 1)
    d[f"{prefix}_ret_4"] = _safe_pct_change(close, 4)
    d[f"{prefix}_ret_24"] = _safe_pct_change(close, 24)

    d[f"{prefix}_vol_24"] = d[f"{prefix}_ret_1"].rolling(24).std()

    d[f"{prefix}_vol_chg_1"] = _safe_pct_change(vol, 1)

    return d

def _prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)
    return df[["ts", "close", "volume"]].copy()

def build_features_leaders_v1(
    btc: pd.DataFrame,
    eth: pd.DataFrame,
    sol: pd.DataFrame,
) -> pd.DataFrame:
    btc = _add_basic_features(_prep(btc), "BTC")
    eth = _add_basic_features(_prep(eth), "ETH")
    sol = _add_basic_features(_prep(sol), "SOL")

    out = btc.merge(
        eth[["ts","ETH_ret_1","ETH_ret_4","ETH_ret_24","ETH_vol_24","ETH_vol_chg_1"]],
        on="ts",
        how="inner",
    ).merge(
        sol[["ts","SOL_ret_1","SOL_ret_4","SOL_ret_24","SOL_vol_24","SOL_vol_chg_1"]],
        on="ts",
        how="inner",
    )

    out = out.sort_values("ts").reset_index(drop=True)
    return out
