import pandas as pd


def _apply_min_hold(signals: pd.Series, hold_bars: int = 24) -> pd.Series:
    s = signals.copy().fillna("flat")
    if len(s) == 0:
        return s

    last = "flat"
    hold = 0
    out = []

    for v in s.tolist():
        if last == "flat" and v == "flat":
            out.append("flat")
            continue

        if last == "flat" and v in ("up", "down"):
            last = v
            hold = 0
            out.append(last)
            continue

        if v == "flat":
            out.append(last)
            hold += 1
            continue

        if v != last and hold >= hold_bars:
            last = v
            hold = 0

        out.append(last)
        hold += 1

    return pd.Series(out, index=s.index, dtype="object")


def _apply_confirm_switch(signals: pd.Series, confirm_bars: int = 2) -> pd.Series:
    s = signals.copy().fillna("flat")
    if len(s) == 0:
        return s

    last = "flat"
    pending = None
    pending_count = 0
    out = []

    for v in s.tolist():
        if v == last:
            pending = None
            pending_count = 0
            out.append(last)
            continue

        if v == "flat":
            pending = None
            pending_count = 0
            out.append(last)
            continue

        if pending is None or pending != v:
            pending = v
            pending_count = 1
            out.append(last)
            continue

        pending_count += 1
        if pending_count >= confirm_bars:
            last = pending
            pending = None
            pending_count = 0

        out.append(last)

    return pd.Series(out, index=s.index, dtype="object")


def apply_signal_filters(raw_signals: pd.Series, confirm_bars: int = 2, hold_bars: int = 24) -> pd.Series:
    if confirm_bars < 1:
        raise ValueError("confirm_bars must be >= 1")
    if hold_bars < 0:
        raise ValueError("hold_bars must be >= 0")

    raw_signals = raw_signals.fillna("flat")
    bad = set(raw_signals.unique()) - {"up", "down", "flat"}
    if bad:
        raise ValueError(f"Unexpected signal values: {bad}")

    confirmed = _apply_confirm_switch(raw_signals, confirm_bars=confirm_bars)
    held = _apply_min_hold(confirmed, hold_bars=hold_bars)
    return held
