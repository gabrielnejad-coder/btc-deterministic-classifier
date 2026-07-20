"""
Feature Pack V2 — Positioning Pressure Proxies.

Hypothesis: forced flow dynamics (approximated from funding + price action)
carry different signal than directional price features.

New features (all causal, trailing only):
  Funding pressure:
    - BTC_funding_accel: 8h change in funding rate
    - BTC_funding_pctl_72h: rolling percentile of funding (crowding detector)
    - ETH_funding_accel: same for ETH
    - ETH_funding_pctl_72h: same for ETH
    - funding_divergence: BTC funding - ETH funding acceleration

  Price action stress:
    - BTC_wick_ratio_upper: upper wick / range (rejection signal)
    - BTC_wick_ratio_lower: lower wick / range
    - BTC_atr_ratio_24h: current ATR vs 24h avg ATR (expansion signal)
    - BTC_vwap_dev_12h: close deviation from rolling 12h VWAP
    - BTC_range_compression_72h: ratio of recent range to 72h range

Total: 15 base (leaders_v1) + 7 funding (pack_v1) + 10 positioning = 32 features
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from features.packs.pack_v1_derivs import build_features_pack_v1, FEATURE_NAMES_PACK_V1


def build_features_pack_v2(
    btc: pd.DataFrame,
    eth: pd.DataFrame,
    sol: pd.DataFrame,
) -> pd.DataFrame:
    """Build pack_v1 features + positioning pressure proxies."""

    # Start with pack_v1 (base + funding)
    df = build_features_pack_v1(btc, eth, sol)
    df = df.sort_values("ts").reset_index(drop=True)

    # We need OHLCV aligned to feature rows
    btc_c = btc.copy()
    btc_c["ts"] = pd.to_datetime(btc_c["ts"], utc=True)
    btc_c = btc_c.set_index("ts").sort_index()

    # Align BTC OHLCV to feature timestamps
    feat_ts = pd.to_datetime(df["ts"], utc=True)
    o = btc_c["open"].reindex(feat_ts).values.astype(float)
    h = btc_c["high"].reindex(feat_ts).values.astype(float)
    l = btc_c["low"].reindex(feat_ts).values.astype(float)
    c = btc_c["close"].reindex(feat_ts).values.astype(float)
    v = btc_c["volume"].reindex(feat_ts).values.astype(float)

    # ── Funding pressure features ────────────────────────────────────

    # Funding acceleration (change in funding rate, approximates OI build)
    for prefix in ["BTC", "ETH"]:
        col = f"{prefix}_funding"
        if col in df.columns:
            df[f"{prefix}_funding_accel"] = df[col].diff(8)  # 8-bar diff = one funding period

            # Rolling percentile (crowding detector)
            roll = df[col].rolling(72)
            rank = df[col].rolling(72).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False)
            df[f"{prefix}_funding_pctl_72h"] = rank

    # Funding divergence (BTC vs ETH acceleration)
    if "BTC_funding_accel" in df.columns and "ETH_funding_accel" in df.columns:
        df["funding_divergence"] = df["BTC_funding_accel"] - df["ETH_funding_accel"]

    # ── Price action stress features ─────────────────────────────────

    bar_range = h - l
    bar_range_safe = np.where(bar_range == 0, np.nan, bar_range)

    # Wick ratios (rejection signals)
    upper_wick = h - np.maximum(o, c)
    lower_wick = np.minimum(o, c) - l
    df["BTC_wick_upper"] = upper_wick / bar_range_safe
    df["BTC_wick_lower"] = lower_wick / bar_range_safe

    # ATR ratio (expansion/compression)
    atr_1 = pd.Series(bar_range).rolling(1).mean().values
    atr_24 = pd.Series(bar_range).rolling(24).mean().values
    atr_24_safe = np.where(atr_24 == 0, np.nan, atr_24)
    df["BTC_atr_ratio_24h"] = atr_1 / atr_24_safe

    # VWAP deviation (forced directional flow)
    vwap_num = pd.Series(c * v).rolling(12).sum().values
    vwap_den = pd.Series(v).rolling(12).sum().values
    vwap_den_safe = np.where(vwap_den == 0, np.nan, vwap_den)
    vwap_12h = vwap_num / vwap_den_safe
    df["BTC_vwap_dev_12h"] = (c - vwap_12h) / vwap_12h

    # Range compression (recent range vs 72h range)
    range_12h = pd.Series(h).rolling(12).max().values - pd.Series(l).rolling(12).min().values
    range_72h = pd.Series(h).rolling(72).max().values - pd.Series(l).rolling(72).min().values
    range_72h_safe = np.where(range_72h == 0, np.nan, range_72h)
    df["BTC_range_compression"] = range_12h / range_72h_safe

    # Replace inf
    df = df.replace([np.inf, -np.inf], np.nan)

    return df


FEATURE_NAMES_PACK_V2 = FEATURE_NAMES_PACK_V1 + [
    "BTC_funding_accel", "BTC_funding_pctl_72h",
    "ETH_funding_accel", "ETH_funding_pctl_72h",
    "funding_divergence",
    "BTC_wick_upper", "BTC_wick_lower",
    "BTC_atr_ratio_24h", "BTC_vwap_dev_12h",
    "BTC_range_compression",
]
