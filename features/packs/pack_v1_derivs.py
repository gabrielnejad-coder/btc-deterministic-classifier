"""
Feature Pack V1 — Derivatives (funding rates only).

Adds to existing leaders_v1 features:
  - BTC_funding: raw funding rate (forward-filled 8h → 1h)
  - ETH_funding: raw funding rate
  - BTC_funding_cum_24h: cumulative funding over last 24 bars
  - ETH_funding_cum_24h: cumulative funding over last 24 bars
  - BTC_funding_z_72h: z-score of funding vs trailing 72h window
  - ETH_funding_z_72h: z-score of funding vs trailing 72h window
  - funding_spread: BTC_funding - ETH_funding (relative positioning)

All computed at bar close. No lookahead.
Forward-fill alignment rule is baked into canonical_derivs_1h.parquet.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from features.build_features_leaders_v1 import build_features_leaders_v1


DATA_DIR = Path("data_parquet")
DERIVS_PATH = DATA_DIR / "canonical_derivs_1h.parquet"


def build_features_pack_v1(
    btc: pd.DataFrame,
    eth: pd.DataFrame,
    sol: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build leaders_v1 features + derivatives features.

    Returns DataFrame with ts + all feature columns.
    NaN rows from rolling windows are NOT dropped here
    (caller decides dropna policy).
    """
    # Base features (15 columns from leaders_v1)
    base = build_features_leaders_v1(btc, eth, sol)

    # Load canonicalized derivatives
    if not DERIVS_PATH.exists():
        raise FileNotFoundError(
            f"Missing {DERIVS_PATH}. Run: python data_scripts/validate_and_canonicalize.py"
        )
    derivs = pd.read_parquet(DERIVS_PATH)
    derivs["ts"] = pd.to_datetime(derivs["ts"], utc=True)

    # Merge on ts (inner join — only bars with both)
    merged = base.merge(derivs, on="ts", how="inner")
    merged = merged.sort_values("ts").reset_index(drop=True)

    # ── Derived features (all causal, trailing only) ─────────────────

    # Cumulative funding over trailing 24h (sum of 8h prints, but on 1h grid
    # the forward-filled values repeat, so we take rolling mean * 24 / 8 = * 3)
    # Simpler and more correct: rolling sum of the raw values over 24 bars.
    # Since funding is forward-filled, we need rolling mean (not sum) to avoid
    # triple-counting the same 8h print.
    merged["BTC_funding_ma_24h"] = merged["BTC_funding"].rolling(24).mean()
    merged["ETH_funding_ma_24h"] = merged["ETH_funding"].rolling(24).mean()

    # Z-score vs trailing 72h window
    for prefix in ["BTC", "ETH"]:
        col = f"{prefix}_funding"
        roll_mean = merged[col].rolling(72).mean()
        roll_std = merged[col].rolling(72).std()
        merged[f"{prefix}_funding_z_72h"] = (merged[col] - roll_mean) / roll_std.replace(0, np.nan)

    # Spread: BTC vs ETH funding (relative crowding)
    merged["funding_spread"] = merged["BTC_funding"] - merged["ETH_funding"]

    # Replace inf with nan
    merged = merged.replace([np.inf, -np.inf], np.nan)

    return merged


# Feature names for the model (base 15 + 7 new = 22 total)
FEATURE_NAMES_PACK_V1 = [
    # Base leaders_v1 (15)
    "BTC_ret_1", "BTC_ret_4", "BTC_ret_24", "BTC_vol_24", "BTC_vol_chg_1",
    "ETH_ret_1", "ETH_ret_4", "ETH_ret_24", "ETH_vol_24", "ETH_vol_chg_1",
    "SOL_ret_1", "SOL_ret_4", "SOL_ret_24", "SOL_vol_24", "SOL_vol_chg_1",
    # Derivatives pack v1 (7)
    "BTC_funding", "ETH_funding",
    "BTC_funding_ma_24h", "ETH_funding_ma_24h",
    "BTC_funding_z_72h", "ETH_funding_z_72h",
    "funding_spread",
]
