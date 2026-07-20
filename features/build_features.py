import pandas as pd
import numpy as np


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d = d.sort_values("ts").reset_index(drop=True)

    d["ret_1"] = d["close"].pct_change(1)
    d["ret_2"] = d["close"].pct_change(2)
    d["ret_4"] = d["close"].pct_change(4)
    d["ret_8"] = d["close"].pct_change(8)
    d["ret_12"] = d["close"].pct_change(12)
    d["ret_24"] = d["close"].pct_change(24)

    d["mom_12"] = d["close"] / d["close"].shift(12) - 1.0

    d["vol_12"] = d["ret_1"].rolling(12).std()
    d["vol_24"] = d["ret_1"].rolling(24).std()
    d["vol_48"] = d["ret_1"].rolling(48).std()

    d["range_1"] = (d["high"] - d["low"]) / d["close"]

    d["vol_chg_1"] = d["volume"] / d["volume"].shift(1) - 1.0

    vmean = d["volume"].rolling(48).mean()
    vstd = d["volume"].rolling(48).std()
    d["vol_z_48"] = (d["volume"] - vmean) / vstd

    cols = [
        "ts",
        "close",
        "ret_1", "ret_2", "ret_4", "ret_8", "ret_12", "ret_24",
        "mom_12",
        "vol_12", "vol_24", "vol_48",
        "range_1",
        "vol_chg_1", "vol_z_48",
    ]

    out = d[cols].replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)
    return out
