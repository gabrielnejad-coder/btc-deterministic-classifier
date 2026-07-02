"""
Always-on runner for the paper traders.

This is a thin supervisor loop. It does NOT touch any trading logic, thresholds,
or the frozen strategy parameters — it just imports the chosen trader module and
calls its existing `_run_tick()` once per hour, forever, surviving transient
errors (e.g. a CryptoCompare hiccup) instead of dying.

Which trader to run is chosen by the TRADER env var:
    TRADER=060  -> live/paper_trader.py      (frozen: vol_pctl=0.60)   [default]
    TRADER=050  -> live/paper_trader_050.py  (vol_pctl=0.50 variant)

Optional STATE_ROOT env var redirects the trader's state + log directories onto
a persistent Railway Volume so equity/position/trade history survive redeploys
and restarts. Without it the trader writes under ./live (ephemeral on Railway).

Run from the repo root:  python -u live/runner.py
"""
from __future__ import annotations

import importlib
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

_MODULES = {"060": "paper_trader", "050": "paper_trader_050"}


def _resolve_trader() -> str:
    """Which strategy to run. Explicit TRADER env wins; otherwise infer from the
    Railway service name (auto-injected as RAILWAY_SERVICE_NAME), so no per-service
    dashboard config is required. Defaults to the frozen 0.60 strategy."""
    explicit = os.environ.get("TRADER")
    if explicit and explicit.strip() in _MODULES:
        return explicit.strip()
    svc = (os.environ.get("RAILWAY_SERVICE_NAME") or "").lower()
    if "050" in svc:
        return "050"
    return "060"


def _resolve_state_root():
    """Where to persist state/logs. Explicit STATE_ROOT env wins; otherwise
    auto-use a mounted Railway volume at /data if present; else None (ephemeral)."""
    root = os.environ.get("STATE_ROOT")
    if root:
        return root
    if os.path.isdir("/data"):
        return "/data"
    return None


TRADER = _resolve_trader()
MODULE_NAME = _MODULES[TRADER]


def _log(msg: str) -> None:
    print(f"[runner {TRADER}] [{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _redirect_state(pt) -> None:
    """Point the trader's state/log dirs at STATE_ROOT (a persistent volume).

    This only reassigns module-level path globals; it does not alter any logic.
    """
    root = _resolve_state_root()
    if not root:
        _log(f"no persistent volume found — writing state under {pt.STATE_DIR.resolve()} "
             f"(EPHEMERAL on Railway; attach a Volume mounted at /data to persist)")
        return
    base = Path(root)
    suffix = "_050" if MODULE_NAME == "paper_trader_050" else ""
    pt.STATE_DIR = base / f"state{suffix}"
    pt.LOG_DIR = base / f"logs{suffix}"
    pt.STATE_FILE = pt.STATE_DIR / "position.json"
    pt.TRADES_LOG = pt.LOG_DIR / "trades.jsonl"
    pt.SIGNALS_LOG = pt.LOG_DIR / "signals.jsonl"
    pt.STATE_DIR.mkdir(parents=True, exist_ok=True)
    pt.LOG_DIR.mkdir(parents=True, exist_ok=True)
    _log(f"state -> {pt.STATE_DIR}  logs -> {pt.LOG_DIR}")


def _seconds_to_next_hour() -> float:
    """Sleep until ~1 minute past the next hour, so the freshest hourly candle
    from CryptoCompare has closed (mirrors the previous hourly cron cadence)."""
    now = datetime.now(timezone.utc)
    nxt = (now + timedelta(hours=1)).replace(minute=1, second=0, microsecond=0)
    return max(60.0, (nxt - now).total_seconds())


def main() -> None:
    pt = importlib.import_module(MODULE_NAME)
    _log(f"starting; module={MODULE_NAME} VOL_PCTL_ENTRY={pt.VOL_PCTL_ENTRY} "
         f"BRK_BARS={pt.BRK_BARS} HOLD_MIN_BARS={pt.HOLD_MIN_BARS} "
         f"STOP_LOSS_PCT={pt.STOP_LOSS_PCT}")
    _log("data source: Kraken public OHLC (keyless, hourly BTC/USD)")
    _redirect_state(pt)

    while True:
        try:
            pt._run_tick()
        except Exception as exc:  # keep the loop alive across transient failures
            _log(f"TICK ERROR ({type(exc).__name__}): {exc}")
        sleep_s = _seconds_to_next_hour()
        _log(f"tick done; sleeping {int(sleep_s)}s until next hourly candle")
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
