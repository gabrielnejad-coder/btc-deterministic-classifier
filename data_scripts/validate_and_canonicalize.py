"""
Step 1: Validate raw funding data
Step 2: Canonicalize to 1h grid (aligned to BTC canonical bars)

Alignment rule (frozen):
  Funding rate is 8h. We forward-fill to the 1h grid.
  Each bar carries the LAST KNOWN funding rate at bar close.
  No lookahead: funding at 08:00 applies to bars 08:00-15:00.

Output:
  data_parquet/canonical_derivs_1h.parquet
    columns: ts, BTC_funding, ETH_funding
    aligned to BTC canonical 1h index
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import numpy as np


DATA_DIR = Path("data_parquet")
REPORTS_DIR = Path("reports")


def _validate_funding(path: Path, expected_freq_hours: int = 8) -> pd.DataFrame:
    """Validate raw funding parquet. Fail loudly on issues."""
    print(f"\n  VALIDATING {path.name}")

    df = pd.read_parquet(path)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    # Basic checks
    assert "ts" in df.columns, "missing ts"
    assert "funding_rate" in df.columns, "missing funding_rate"
    assert df["ts"].dt.tz is not None, "ts must be UTC"

    # Duplicates
    dupes = df["ts"].duplicated().sum()
    print(f"    rows: {len(df)}")
    print(f"    range: {df['ts'].min()} to {df['ts'].max()}")
    print(f"    dupes: {dupes}")
    assert dupes == 0, f"found {dupes} duplicate timestamps"

    # Frequency check (should be ~8h, allow some variance)
    diffs = df["ts"].diff().dropna()
    median_diff_hours = diffs.median().total_seconds() / 3600
    print(f"    median_interval: {median_diff_hours:.1f}h (expected ~{expected_freq_hours}h)")
    assert abs(median_diff_hours - expected_freq_hours) < 1.0, (
        f"unexpected interval: {median_diff_hours}h"
    )

    # NaN check
    nan_count = df["funding_rate"].isna().sum()
    print(f"    nan_rates: {nan_count}")

    # Value range sanity (funding rates are typically -0.01 to +0.01)
    fr = df["funding_rate"]
    print(f"    rate range: [{fr.min():.6f}, {fr.max():.6f}]")
    print(f"    rate mean: {fr.mean():.6f}")

    # Timestamp precision fix: snap to nearest hour
    # (raw data has millisecond offsets like 00:00:00.006)
    df["ts"] = df["ts"].dt.floor("h")
    dupes_after_snap = df["ts"].duplicated().sum()
    if dupes_after_snap > 0:
        print(f"    dupes after hour-snap: {dupes_after_snap} (taking last)")
        df = df.drop_duplicates("ts", keep="last").reset_index(drop=True)

    print(f"    VALID")
    return df


def _load_canonical_index() -> pd.DatetimeIndex:
    """Load the canonical BTC 1h bar timestamps."""
    btc = pd.read_parquet(DATA_DIR / "BTCUSD_USD_1h_20220323_now.parquet")
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts")
    return pd.DatetimeIndex(btc["ts"], name="ts")


def _forward_fill_to_1h(
    funding_df: pd.DataFrame,
    canonical_idx: pd.DatetimeIndex,
    col_name: str,
) -> pd.Series:
    """
    Forward-fill 8h funding rate onto 1h grid.

    Rule: each 1h bar gets the LAST KNOWN funding rate.
    If funding prints at 08:00, bars 08:00 through 15:00 carry that rate.
    Bar 16:00 gets the 16:00 print (next funding).

    This is strictly causal — no lookahead.
    """
    fr_series = funding_df.set_index("ts")["funding_rate"]
    fr_series = fr_series[~fr_series.index.duplicated(keep="last")]

    # Reindex to canonical, forward-fill
    aligned = fr_series.reindex(canonical_idx, method="ffill")
    aligned.name = col_name

    return aligned


def _compute_hash(df: pd.DataFrame) -> str:
    """Deterministic hash of the output for audit."""
    content = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(content).hexdigest()[:16]


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Validate raw data ────────────────────────────────────────────
    btc_fr = _validate_funding(DATA_DIR / "BTC_funding_8h.parquet")
    eth_fr = _validate_funding(DATA_DIR / "ETH_funding_8h.parquet")

    # ── Load canonical 1h index ──────────────────────────────────────
    canonical_idx = _load_canonical_index()
    print(f"\n  CANONICAL INDEX")
    print(f"    bars: {len(canonical_idx)}")
    print(f"    range: {canonical_idx.min()} to {canonical_idx.max()}")

    # ── Forward-fill to 1h grid ──────────────────────────────────────
    btc_funding_1h = _forward_fill_to_1h(btc_fr, canonical_idx, "BTC_funding")
    eth_funding_1h = _forward_fill_to_1h(eth_fr, canonical_idx, "ETH_funding")

    # ── Assemble output ──────────────────────────────────────────────
    out = pd.DataFrame({
        "ts": canonical_idx,
        "BTC_funding": btc_funding_1h.values,
        "ETH_funding": eth_funding_1h.values,
    })

    # ── Report coverage ──────────────────────────────────────────────
    btc_nan = out["BTC_funding"].isna().sum()
    eth_nan = out["ETH_funding"].isna().sum()
    btc_first_valid = out.loc[out["BTC_funding"].notna(), "ts"].min()
    eth_first_valid = out.loc[out["ETH_funding"].notna(), "ts"].min()

    print(f"\n  CANONICALIZED OUTPUT")
    print(f"    rows: {len(out)}")
    print(f"    BTC_funding NaN: {btc_nan} (first valid: {btc_first_valid})")
    print(f"    ETH_funding NaN: {eth_nan} (first valid: {eth_first_valid})")
    print(f"    BTC_funding range: [{out['BTC_funding'].min():.6f}, {out['BTC_funding'].max():.6f}]")
    print(f"    ETH_funding range: [{out['ETH_funding'].min():.6f}, {out['ETH_funding'].max():.6f}]")

    # ── Save ─────────────────────────────────────────────────────────
    out_path = DATA_DIR / "canonical_derivs_1h.parquet"
    out.to_parquet(out_path, index=False)
    print(f"\n    WROTE {out_path}")

    # ── Audit manifest ───────────────────────────────────────────────
    data_hash = _compute_hash(out)
    manifest = {
        "version": "derivs_v1",
        "alignment_rule": "forward_fill_8h_to_1h_causal",
        "gap_bars": 0,
        "canonical_source": "BTCUSD_USD_1h_20220323_now.parquet",
        "inputs": {
            "BTC_funding_8h": str(DATA_DIR / "BTC_funding_8h.parquet"),
            "ETH_funding_8h": str(DATA_DIR / "ETH_funding_8h.parquet"),
        },
        "output": str(out_path),
        "output_hash": data_hash,
        "rows": len(out),
        "columns": list(out.columns),
        "btc_funding_nan_count": int(btc_nan),
        "eth_funding_nan_count": int(eth_nan),
    }
    manifest_path = REPORTS_DIR / "derivs_v1_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"    WROTE {manifest_path}")
    print(f"    hash: {data_hash}")


if __name__ == "__main__":
    main()
