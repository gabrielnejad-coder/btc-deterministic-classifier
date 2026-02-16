import pandas as pd

from model.strategy_v1 import build_signals_v1
from backtest.engine import EngineConfig, run_engine


def test_strategy_v1_wires_into_engine():
    df = pd.DataFrame(
        {
            "ts": pd.date_range("2024-01-01", periods=80, freq="h", tz="UTC"),
            "open": [100 + i for i in range(80)],
            "high": [101 + i for i in range(80)],
            "low": [99 + i for i in range(80)],
            "close": [100.5 + i for i in range(80)],
            "volume": [1.0 for _ in range(80)],
        }
    )

    sig = build_signals_v1(df)

    expected_index = pd.DatetimeIndex(pd.to_datetime(df["ts"], utc=True))
    assert sig.index.equals(expected_index)

    cfg = EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        initial_equity=1.0,
    )

    trades, equity, metrics = run_engine(df, sig, cfg)

    assert trades is not None
    assert equity is not None
    assert len(equity) > 0
    assert "equity" in equity.columns
    assert metrics is not None
