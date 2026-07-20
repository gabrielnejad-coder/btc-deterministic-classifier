import pandas as pd

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from model.strategy_v2 import build_signals_v2


def main():
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet")
    test_df = split_walkforward(df)["test"].copy()

    sig = build_signals_v2(test_df)

    cfg_free = EngineConfig(
        fee_taker=0.0,
        slippage_side=0.0,
        stop_loss_pct=0.02,
        hold_min_bars=12,
        initial_equity=1000.0,
    )

    cfg_cost = EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        hold_min_bars=12,
        initial_equity=1000.0,
    )

    trades0, equity0, m0 = run_engine(test_df, sig, cfg_free)
    trades1, equity1, m1 = run_engine(test_df, sig, cfg_cost)

    print("TEST_DIAGNOSE")
    print("bars", len(test_df))
    print("signals_counts", sig.value_counts(dropna=False).to_dict())
    print()
    print("NO_COST", m0)
    print("WITH_COST", m1)
    print()
    print("trades_cols", list(trades1.columns))

    if len(trades1) > 0:
        print("num_trades", len(trades1))

        # Compute PnL from actual columns
        completed = trades1.dropna(subset=["entry_px", "exit_px"])
        if len(completed) > 0:
            long_mask = completed["side"] == "long"
            short_mask = completed["side"] == "short"

            pnl = pd.Series(index=completed.index, dtype=float)
            pnl[long_mask] = (completed.loc[long_mask, "exit_px"] / completed.loc[long_mask, "entry_px"]) - 1
            pnl[short_mask] = (completed.loc[short_mask, "entry_px"] / completed.loc[short_mask, "exit_px"]) - 1

            total_fees = completed["fee_entry"] + completed["fee_exit"]

            print()
            print("completed_trades", len(completed))
            print("avg_gross_pnl_pct", f"{pnl.mean() * 100:.4f}%")
            print("avg_fees", f"{total_fees.mean():.4f}")
            print("win_rate", f"{(pnl > 0).mean() * 100:.1f}%")
            print("exit_reasons", completed["exit_reason"].value_counts().to_dict())
        else:
            print("no_completed_trades")
    else:
        print("no_trades")


if __name__ == "__main__":
    main()
