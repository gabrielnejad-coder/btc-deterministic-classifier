from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
import inspect
import pandas as pd

def _load_engine() -> Tuple[Any, Optional[Any]]:
    candidates = [
        ("backtest.engine", "run_engine", "EngineConfig"),
        ("engine", "run_engine", "EngineConfig"),
    ]
    last_err: Optional[Exception] = None
    for mod_name, fn_name, cfg_name in candidates:
        try:
            mod = __import__(mod_name, fromlist=[fn_name, cfg_name])
            fn = getattr(mod, fn_name, None)
            cfg = getattr(mod, cfg_name, None)
            if callable(fn):
                return fn, cfg
        except Exception as e:
            last_err = e
    raise ModuleNotFoundError(f"Could not locate deterministic engine run_engine. Last error: {last_err}")

def _default_engine_cfg_dict(initial_equity: float) -> Dict[str, Any]:
    return {
        "fee_taker": 0.0004,
        "slippage_side": 0.0001,
        "stop_loss_pct": 0.02,
        "initial_equity": float(initial_equity),
        "one_position": True,
    }

def _make_cfg(cfg_cls: Optional[Any], initial_equity: float) -> Any:
    d = _default_engine_cfg_dict(initial_equity)
    if cfg_cls is None:
        return d
    try:
        return cfg_cls(**d)
    except Exception:
        return d

def _normalize_engine_output(out: Any) -> Dict[str, Any]:
    if isinstance(out, dict):
        return out
    if isinstance(out, tuple) and len(out) >= 3 and isinstance(out[-1], dict):
        return out[-1]
    raise TypeError(f"engine returned {type(out)}, expected dict or (.., .., metrics_dict)")

def _call_engine(run_engine_fn: Any, df: pd.DataFrame, signals: pd.Series, cfg_obj: Any) -> Dict[str, Any]:
    sig = inspect.signature(run_engine_fn)
    params = list(sig.parameters.keys())
    if len(params) >= 3:
        p3 = params[2]
        out = run_engine_fn(df, signals, cfg=cfg_obj) if p3 == "cfg" else run_engine_fn(df, signals, cfg_obj)
        return _normalize_engine_output(out)
    if len(params) == 2:
        p2 = params[1]
        out = run_engine_fn(df, cfg=cfg_obj) if p2 == "cfg" else run_engine_fn(df, signals)
        return _normalize_engine_output(out)
    return _normalize_engine_output(run_engine_fn(df))

def run_signals_through_engine(
    bars: pd.DataFrame,
    signals: pd.DataFrame,
    policy_name: str = "v3_prob",
    trade_threshold: float = 0.45,
    flip_threshold: float = 0.45,
    initial_equity: float = 1000.0,
) -> Dict[str, Any]:
    if "ts" not in bars.columns:
        raise ValueError("bars missing ts")
    if "ts" not in signals.columns or "signal" not in signals.columns:
        raise ValueError("signals must have columns ts and signal")

    df = bars.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    s = signals.copy()
    s["ts"] = pd.to_datetime(s["ts"], utc=True)
    s = s.sort_values("ts").reset_index(drop=True)

    merged = df.merge(s[["ts", "signal"]], on="ts", how="left")
    merged["signal"] = merged["signal"].fillna("flat")
    merged["policy_name"] = str(policy_name)
    merged["trade_threshold"] = float(trade_threshold)
    merged["flip_threshold"] = float(flip_threshold)

    ts_index = pd.to_datetime(merged["ts"].to_numpy(), utc=True)
    signals_series = pd.Series(merged["signal"].to_numpy(), index=ts_index, name="signal")

    run_engine_fn, cfg_cls = _load_engine()
    cfg_obj = _make_cfg(cfg_cls, initial_equity=initial_equity)

    metrics = _call_engine(run_engine_fn, merged, signals_series, cfg_obj)
    for k in ("num_trades", "total_return", "max_drawdown"):
        if k not in metrics:
            raise KeyError(f"engine metrics missing key: {k}")
    return metrics
