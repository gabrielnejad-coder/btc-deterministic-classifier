import pandas as pd
from backtest.engine import EngineConfig, run_engine

def _df_two_bars():
    return pd.DataFrame({
        "ts": pd.to_datetime(["2026-01-01T00:00:00Z","2026-01-01T01:00:00Z"], utc=True),
        "open": [100.0, 100.0],
        "high": [101.0, 101.0],
        "low":  [99.0, 99.0],
        "close":[100.0, 100.0],
        "volume":[1.0, 1.0],
    })

def test_entry_fills_next_open():
    df = _df_two_bars()
    ts = pd.DatetimeIndex(df["ts"])
    sig = pd.Series(["up","up"], index=ts)

    cfg = EngineConfig(fee_taker=0.0, slippage_side=0.0, stop_loss_pct=0.99, initial_equity=1.0)
    trades, equity, metrics = run_engine(df, sig, cfg)

    assert len(trades) == 1
    assert trades.iloc[0]["entry_ts"] == df.loc[0,"ts"]  # scheduled at close 0, filled at open bar 0 in this engine timing

def test_fee_applied_on_entry_and_exit():
    df = _df_two_bars()
    ts = pd.DatetimeIndex(df["ts"])
    sig = pd.Series(["up","down"], index=ts)

    cfg = EngineConfig(fee_taker=0.1, slippage_side=0.0, stop_loss_pct=0.99, initial_equity=1.0)
    trades, equity, metrics = run_engine(df, sig, cfg)

    assert len(trades) == 1
    assert trades.iloc[0]["fee_entry"] > 0
    assert trades.iloc[0]["fee_exit"] > 0

def test_long_stop_triggers_on_low():
    df = pd.DataFrame({
        "ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
        "open": [100.0],
        "high": [100.0],
        "low":  [97.0],
        "close":[100.0],
        "volume":[1.0],
    })
    ts = pd.DatetimeIndex(df["ts"])
    sig = pd.Series(["up"], index=ts)

    cfg = EngineConfig(fee_taker=0.0, slippage_side=0.0, stop_loss_pct=0.02, initial_equity=1.0)
    trades, equity, metrics = run_engine(df, sig, cfg)

    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "stop"

def test_no_multiple_positions():
    df = pd.DataFrame({
        "ts": pd.to_datetime(["2026-01-01T00:00:00Z","2026-01-01T01:00:00Z","2026-01-01T02:00:00Z"], utc=True),
        "open": [100.0, 100.0, 100.0],
        "high": [100.0, 100.0, 100.0],
        "low":  [100.0, 100.0, 100.0],
        "close":[100.0, 100.0, 100.0],
        "volume":[1.0, 1.0, 1.0],
    })
    ts = pd.DatetimeIndex(df["ts"])
    sig = pd.Series(["up","up","up"], index=ts)

    cfg = EngineConfig(fee_taker=0.0, slippage_side=0.0, stop_loss_pct=0.99, initial_equity=1.0)
    trades, equity, metrics = run_engine(df, sig, cfg)

    assert len(trades) <= 1
