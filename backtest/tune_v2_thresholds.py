import numpy as np
import pandas as pd

from backtest.walkforward import split_walkforward
from backtest.engine import EngineConfig, run_engine
from model.strategy_v2 import build_signals_v2


def load_cfg() -> EngineConfig:
    from pathlib import Path
    p = Path("config/v1.yaml")
    txt = p.read_text()
    try:
        import yaml  # type: ignore
        cfg = yaml.safe_load(txt)
    except Exception:
        cfg = {}
        for line in txt.splitlines():
            s = line.strip()
            if not s or s.startswith("#") or ":" not in s:
                continue
            k, v = s.split(":", 1)
            cfg[k.strip()] = v.strip()

    def f(key, default):
        v = cfg.get(key, default)
        try:
            return float(v)
        except Exception:
            return float(default)

    def i(key, default):
        v = cfg.get(key, default)
        try:
            return int(float(v))
        except Exception:
            return int(default)

    def b(key, default):
        v = str(cfg.get(key, default)).strip().lower()
        if v in ("true", "1", "yes", "y"):
            return True
        if v in ("false", "0", "no", "n"):
            return False
        return bool(default)

    return EngineConfig(
        fee_taker=f("fee", 0.0004),
        slippage_side=f("slippage", 0.0001),
        stop_loss_pct=f("stop_loss", 0.02),
        hold_min_bars=i("hold_min_bars", 12),
        initial_equity=f("initial_equity", 1000.0),
        one_position=b("one_position_only", True),
    )


def main():
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(df)
    vdf = splits["validate"].copy().reset_index(drop=True)
    cfg = load_cfg()

    min_trades = 10
    max_dd = 0.10

    trade_grid = np.round(np.arange(0.50, 0.81, 0.05), 2)
    flip_grid = np.round(np.arange(0.55, 0.91, 0.05), 2)

    best = None
    tried = 0
    feasible = 0

    for trade_th in trade_grid:
        for flip_th in flip_grid:
            if flip_th < trade_th:
                continue

            tried += 1

            sig = build_signals_v2(
                vdf,
                trade_threshold=float(trade_th),
                flip_threshold=float(flip_th),
            )
            t, e, m = run_engine(vdf, sig, cfg)

            final_eq = float(m["final_equity"])
            dd = float(m["max_drawdown"])
            ntr = int(m["num_trades"])

            if dd > max_dd:
                continue
            if ntr < min_trades:
                continue

            feasible += 1
            score = final_eq

            if best is None or score > best["score"]:
                best = {
                    "trade_threshold": float(trade_th),
                    "flip_threshold": float(flip_th),
                    "final_equity": final_eq,
                    "max_drawdown": dd,
                    "num_trades": ntr,
                    "score": score,
                }

    print("TRIED", tried, "FEASIBLE", feasible)
    print("BEST_VALIDATE_CONSTRAINED")
    print(best)


if __name__ == "__main__":
    main()
