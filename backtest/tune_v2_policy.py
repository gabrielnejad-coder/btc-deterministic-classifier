import json
from pathlib import Path

import pandas as pd

from backtest.engine import EngineConfig, run_engine
from model.strategy_v2 import build_signals_v2


def tune_thresholds_on_validate(
    validate_df: pd.DataFrame,
    cfg: EngineConfig,
    trade_grid: list[float] | None = None,
    flip_grid: list[float] | None = None,
) -> dict:
    if trade_grid is None:
        trade_grid = [round(x, 2) for x in [0.50, 0.52, 0.54, 0.56, 0.58, 0.60, 0.62, 0.64, 0.66, 0.68, 0.70]]
    if flip_grid is None:
        flip_grid = [round(x, 2) for x in [0.60, 0.62, 0.64, 0.66, 0.68, 0.70, 0.72, 0.74, 0.76, 0.78, 0.80]]

    best = None

    for trade_th in trade_grid:
        for flip_th in flip_grid:
            if flip_th < trade_th:
                continue

            signals = build_signals_v2(
                validate_df,
                trade_threshold=trade_th,
                flip_threshold=flip_th,
            )
            trades, equity, metrics = run_engine(validate_df, signals, cfg)

            cand = {
                "trade_threshold": float(trade_th),
                "flip_threshold": float(flip_th),
                "final_equity": float(metrics["final_equity"]),
                "max_drawdown": float(metrics["max_drawdown"]),
                "num_trades": int(metrics["num_trades"]),
            }

            if best is None:
                best = cand
                continue

            best_pass = best["max_drawdown"] <= 0.10
            cand_pass = cand["max_drawdown"] <= 0.10

            if cand_pass and best_pass:
                if cand["final_equity"] > best["final_equity"]:
                    best = cand
            elif cand_pass and not best_pass:
                best = cand
            elif (not cand_pass) and (not best_pass):
                if cand["max_drawdown"] < best["max_drawdown"]:
                    best = cand
                elif cand["max_drawdown"] == best["max_drawdown"] and cand["final_equity"] > best["final_equity"]:
                    best = cand

    if best is None:
        raise RuntimeError("no threshold candidates evaluated")

    return best


def main():
    Path("reports").mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet")
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts")

    from backtest.walkforward import split_walkforward

    splits = split_walkforward(df)
    validate_df = splits["validate"]

    cfg = EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        initial_equity=1_000.0,
    )

    best = tune_thresholds_on_validate(validate_df, cfg)

    out_path = Path("reports/v2_policy_thresholds.json")
    out_path.write_text(json.dumps(best, indent=2))
    print("TUNED_V2_POLICY")
    print(json.dumps(best, indent=2))


if __name__ == "__main__":
    main()
