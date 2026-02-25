from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class EngineConfig:
    fee_taker: float
    slippage_side: float
    stop_loss_pct: float
    initial_equity: float
    hold_min_bars: int = 12
    one_position: bool = True

def _apply_fill_price(direction: str, raw_price: float, slip: float) -> float:
    if direction == "long_entry":
        return raw_price * (1.0 + slip)
    if direction == "long_exit":
        return raw_price * (1.0 - slip)
    if direction == "short_entry":
        return raw_price * (1.0 - slip)
    if direction == "short_exit":
        return raw_price * (1.0 + slip)
    raise ValueError(f"bad direction: {direction}")


def run_engine(
    df: pd.DataFrame,
    signals: pd.Series,
    cfg: EngineConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    required_cols = {"ts", "open", "high", "low", "close", "volume"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"df missing cols: {missing}")

    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    sig = signals.copy()
    if not isinstance(sig.index, pd.DatetimeIndex):
        raise ValueError("signals index must be DateTimeIndex aligned to df ts")
    sig.index = pd.to_datetime(sig.index, utc=True)
    sig = sig.reindex(df["ts"]).fillna("flat").astype("object")

    equity = float(cfg.initial_equity)
    peak = float(cfg.initial_equity)

    position: str = "flat"
    entry_px: Optional[float] = None
    entry_bar_idx: Optional[int] = None
    active_trade_idx: Optional[int] = None

    trades: List[Dict[str, Any]] = []
    equity_rows: List[Dict[str, Any]] = []

    def record_equity(ts_val: pd.Timestamp) -> None:
        nonlocal peak
        peak = max(peak, equity)
        dd = (peak - equity) / peak if peak > 0 else 0.0
        equity_rows.append(
            {"ts": ts_val, "equity": float(equity), "peak": float(peak), "drawdown": float(dd)}
        )

    def _new_trade(decision_ts: pd.Timestamp, side: str) -> int:
        trades.append(
            {
                "decision_ts": decision_ts,
                "side": side,
                "entry_ts": None,
                "entry_raw_px": None,
                "entry_px": None,
                "entry_bar_idx": None,
                "equity_before_entry": None,
                "fee_entry": 0.0,
                "exit_ts": None,
                "exit_raw_px": None,
                "exit_px": None,
                "exit_bar_idx": None,
                "equity_after_exit": None,
                "fee_exit": 0.0,
                "exit_reason": None,
                "gross_ret": None,
                "fees_total": None,
                "net_pnl_dollars": None,
                "net_ret": None,
                "bars_held": None,
            }
        )
        return len(trades) - 1

    def finalize_trade(
        trade_idx: int,
        exit_bar_idx: int,
        exit_ts: pd.Timestamp,
        exit_raw_px: float,
        exit_px_filled: float,
        fee_exit: float,
        exit_reason: str,
        gross_ret: float,
    ) -> None:
        nonlocal equity
        t = trades[trade_idx]
        t["exit_ts"] = exit_ts
        t["exit_raw_px"] = float(exit_raw_px)
        t["exit_px"] = float(exit_px_filled)
        t["exit_bar_idx"] = int(exit_bar_idx)
        t["fee_exit"] = float(fee_exit)
        t["exit_reason"] = exit_reason
        t["equity_after_exit"] = float(equity)
        t["gross_ret"] = float(gross_ret)

        fees_total = float(t["fee_entry"]) + float(fee_exit)
        t["fees_total"] = float(fees_total)

        eq_before = float(t["equity_before_entry"]) if t["equity_before_entry"] is not None else 0.0
        net_pnl = float(equity) - eq_before
        t["net_pnl_dollars"] = float(net_pnl)
        t["net_ret"] = float(net_pnl / eq_before) if eq_before > 0 else 0.0

        if t["entry_bar_idx"] is not None:
            t["bars_held"] = int(exit_bar_idx - int(t["entry_bar_idx"]))

    n = len(df)

    for i in range(n):
        exited_this_bar = False

        ts = df.loc[i, "ts"]
        o = float(df.loc[i, "open"])
        h = float(df.loc[i, "high"])
        l = float(df.loc[i, "low"])

        s = str(sig.iloc[i])
        bars_in_pos = i - entry_bar_idx if entry_bar_idx is not None else 0
        can_signal_exit = bars_in_pos >= int(cfg.hold_min_bars)

        # A) Signal exits at open[i] (guarded by hold_min_bars)
        if position == "long" and can_signal_exit and s in ("down", "flat"):
            exit_px = _apply_fill_price("long_exit", o, cfg.slippage_side)
            gross_ret = (exit_px / float(entry_px)) - 1.0

            equity *= (1.0 + gross_ret)
            fee_exit = equity * float(cfg.fee_taker)
            equity -= fee_exit

            finalize_trade(active_trade_idx, i, ts, o, exit_px, fee_exit, "signal", gross_ret)
            position, entry_px, entry_bar_idx, active_trade_idx = "flat", None, None, None
            exited_this_bar = True

        elif position == "short" and can_signal_exit and s in ("up", "flat"):
            exit_px = _apply_fill_price("short_exit", o, cfg.slippage_side)
            gross_ret = (float(entry_px) / exit_px) - 1.0

            equity *= (1.0 + gross_ret)
            fee_exit = equity * float(cfg.fee_taker)
            equity -= fee_exit

            finalize_trade(active_trade_idx, i, ts, o, exit_px, fee_exit, "signal", gross_ret)
            position, entry_px, entry_bar_idx, active_trade_idx = "flat", None, None, None
            exited_this_bar = True

        # B) Entries at open[i] (only if flat and we did not exit this bar)
        if position == "flat" and (not exited_this_bar):
            if s == "up":
                trade_idx = _new_trade(ts, "long")
                t = trades[trade_idx]
                t["equity_before_entry"] = float(equity)
                t["entry_ts"] = ts
                t["entry_raw_px"] = float(o)
                t["entry_bar_idx"] = int(i)

                fill_px = _apply_fill_price("long_entry", o, cfg.slippage_side)
                fee_entry = equity * float(cfg.fee_taker)
                equity -= fee_entry

                t["entry_px"] = float(fill_px)
                t["fee_entry"] = float(fee_entry)

                position, entry_px, entry_bar_idx, active_trade_idx = "long", float(fill_px), int(i), trade_idx

            elif s == "down":
                trade_idx = _new_trade(ts, "short")
                t = trades[trade_idx]
                t["equity_before_entry"] = float(equity)
                t["entry_ts"] = ts
                t["entry_raw_px"] = float(o)
                t["entry_bar_idx"] = int(i)

                fill_px = _apply_fill_price("short_entry", o, cfg.slippage_side)
                fee_entry = equity * float(cfg.fee_taker)
                equity -= fee_entry

                t["entry_px"] = float(fill_px)
                t["fee_entry"] = float(fee_entry)

                position, entry_px, entry_bar_idx, active_trade_idx = "short", float(fill_px), int(i), trade_idx

        # C) Stops (can exit any time)
        if position == "long":
            stop_px = float(entry_px) * (1.0 - float(cfg.stop_loss_pct))
            if l <= stop_px:
                stop_fill = stop_px * (1.0 - float(cfg.slippage_side))
                gross_ret = (stop_fill / float(entry_px)) - 1.0

                equity *= (1.0 + gross_ret)
                fee_exit = equity * float(cfg.fee_taker)
                equity -= fee_exit

                finalize_trade(active_trade_idx, i, ts, stop_px, stop_fill, fee_exit, "stop", gross_ret)
                position, entry_px, entry_bar_idx, active_trade_idx = "flat", None, None, None
            exited_this_bar = True

        elif position == "short":
            stop_px = float(entry_px) * (1.0 + float(cfg.stop_loss_pct))
            if h >= stop_px:
                stop_fill = stop_px * (1.0 + float(cfg.slippage_side))
                gross_ret = (float(entry_px) / stop_fill) - 1.0

                equity *= (1.0 + gross_ret)
                fee_exit = equity * float(cfg.fee_taker)
                equity -= fee_exit

                finalize_trade(active_trade_idx, i, ts, stop_px, stop_fill, fee_exit, "stop", gross_ret)
                position, entry_px, entry_bar_idx, active_trade_idx = "flat", None, None, None
            exited_this_bar = True

        record_equity(ts)


    # D) Force close any open position at end of data (EOD liquidation)
    if position in ("long", "short") and entry_px is not None and active_trade_idx is not None:
        last_i = n - 1
        last_ts = df.loc[last_i, "ts"]
        last_close = float(df.loc[last_i, "close"])

        if position == "long":
            exit_px = _apply_fill_price("long_exit", last_close, cfg.slippage_side)
            gross_ret = (exit_px / float(entry_px)) - 1.0
        else:
            exit_px = _apply_fill_price("short_exit", last_close, cfg.slippage_side)
            gross_ret = (float(entry_px) / exit_px) - 1.0

        equity *= (1.0 + gross_ret)
        fee_exit = equity * float(cfg.fee_taker)
        equity -= fee_exit

        finalize_trade(active_trade_idx, last_i, last_ts, last_close, exit_px, fee_exit, "eod", gross_ret)
        position, entry_px, entry_bar_idx, active_trade_idx = "flat", None, None, None

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_rows)

    if not trades_df.empty:
        for col in ["decision_ts", "entry_ts", "exit_ts"]:
            trades_df[col] = pd.to_datetime(trades_df[col], utc=True, errors="coerce")

    if not equity_df.empty:
        equity_df["ts"] = pd.to_datetime(equity_df["ts"], utc=True)

    max_dd = float(equity_df["drawdown"].max()) if not equity_df.empty else 0.0
    final_equity = float(equity_df["equity"].iloc[-1]) if not equity_df.empty else float(cfg.initial_equity)
    total_ret = float(final_equity - float(cfg.initial_equity))

    completed = trades_df.dropna(subset=["exit_px"]) if not trades_df.empty else trades_df

    metrics: Dict[str, Any] = {
        "initial_equity": float(cfg.initial_equity),
        "final_equity": float(final_equity),
        "total_return": float(total_ret),
        "max_drawdown": float(max_dd),
        "num_trades": int(len(trades_df)),
        "num_completed": int(len(completed)) if not trades_df.empty else 0,
        "num_wins": int((completed["gross_ret"] > 0).sum()) if not completed.empty else 0,
        "avg_fees": float(completed["fees_total"].mean()) if not completed.empty else 0.0,
        "total_fees": float(completed["fees_total"].sum()) if not completed.empty else 0.0,
    }

    return trades_df, equity_df, metrics
