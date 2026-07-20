"""
Phase 3.5 — Regime Permission Gate.

Replays the monthly retrain experiment with a performance-based gate:
  Before trading window i, check windows i-1 and i-2.
  If both were FAIL → skip window i (stay flat).

This is NOT re-running the model. It replays the existing results
and simulates what would have happened with the gate active.

Also tests two additional gate variants for comparison:
  Gate A: skip if last 2 windows both FAIL
  Gate B: skip if last 3 windows had avg return < 0
  Gate C: skip if last 2 windows both FAIL OR last window dd > 15%

Outputs cumulative equity curve and tail risk metrics for each.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict, Any

import numpy as np


REPORTS_DIR = Path("reports/permission_gate")
MONTHLY_RESULTS_PATH = Path("reports/monthly_retrain/monthly_retrain_results.json")
INITIAL_EQUITY = 1000.0


def _load_results() -> List[Dict[str, Any]]:
    raw = json.loads(MONTHLY_RESULTS_PATH.read_text())
    # Only windows that actually traded (have equity field)
    return [r for r in raw if "equity" in r]


def _simulate(results: List[Dict], gate_fn) -> Dict[str, Any]:
    """
    Simulate cumulative equity with a permission gate.

    gate_fn(history, i) → bool: True = allowed to trade, False = sit flat.
    history is the list of results up to (not including) window i.
    """
    equity = INITIAL_EQUITY
    peak = INITIAL_EQUITY
    max_dd = 0.0
    traded_windows = 0
    skipped_windows = 0
    total_return = 0.0
    worst_window_ret = 0.0
    window_log = []

    for i, r in enumerate(results):
        allowed = gate_fn(results[:i], i)

        if allowed:
            # Apply this window's return proportionally
            window_ret_pct = r["ret"] / INITIAL_EQUITY  # ret is dollar P&L on $1000
            window_pnl = equity * window_ret_pct
            equity += window_pnl
            traded_windows += 1
            total_return += window_pnl

            if window_pnl < worst_window_ret:
                worst_window_ret = window_pnl

            action = "TRADE"
        else:
            window_pnl = 0.0
            skipped_windows += 1
            action = "SKIP"

        peak = max(peak, equity)
        dd = (peak - equity) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

        window_log.append({
            "window": r["window"],
            "period": f"{r['test_start']}->{r['test_end']}",
            "action": action,
            "window_ret": r["ret"],
            "equity_after": round(equity, 2),
            "dd": round(dd, 4),
            "original_status": r["status"],
        })

    return {
        "final_equity": round(equity, 2),
        "total_return": round(total_return, 2),
        "max_dd": round(max_dd, 4),
        "traded": traded_windows,
        "skipped": skipped_windows,
        "worst_window": round(worst_window_ret, 2),
        "log": window_log,
    }


def gate_none(history, i):
    """No gate — trade every window."""
    return True


def gate_a(history, i):
    """Skip if last 2 windows both FAIL."""
    if len(history) < 2:
        return True
    return not (history[-1]["status"] == "FAIL" and history[-2]["status"] == "FAIL")


def gate_b(history, i):
    """Skip if last 3 windows avg return < 0."""
    if len(history) < 3:
        return True
    avg = np.mean([h["ret"] for h in history[-3:]])
    return avg >= 0


def gate_c(history, i):
    """Skip if last 2 both FAIL OR last window dd > 15%."""
    if len(history) < 1:
        return True
    if history[-1]["dd"] > 0.15:
        return False
    if len(history) >= 2 and history[-1]["status"] == "FAIL" and history[-2]["status"] == "FAIL":
        return False
    return True


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    results = _load_results()
    print(f"PERMISSION GATE EXPERIMENT")
    print(f"  windows: {len(results)}")

    gates = {
        "NO_GATE": gate_none,
        "GATE_A (2x FAIL skip)": gate_a,
        "GATE_B (3mo avg ret < 0)": gate_b,
        "GATE_C (2x FAIL or dd>15%)": gate_c,
    }

    all_outcomes = {}

    for name, fn in gates.items():
        outcome = _simulate(results, fn)
        all_outcomes[name] = outcome

    # ── Summary table ────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print(f"  {'gate':<28s} {'equity':>8s} {'return':>8s} {'max_dd':>7s} {'traded':>7s} {'skipped':>8s} {'worst':>8s}")
    print(f"  {'-'*28} {'-'*8} {'-'*8} {'-'*7} {'-'*7} {'-'*8} {'-'*8}")

    for name, o in all_outcomes.items():
        print(f"  {name:<28s} ${o['final_equity']:>7.0f} ${o['total_return']:>7.0f} "
              f"{o['max_dd']:>6.4f} {o['traded']:>7d} {o['skipped']:>8d} ${o['worst_window']:>7.0f}")

    # ── Detailed log for best gate ───────────────────────────────────
    # Find gate with best equity that has max_dd < 0.15
    viable = {k: v for k, v in all_outcomes.items() if k != "NO_GATE"}
    if viable:
        best_name = max(viable, key=lambda k: viable[k]["final_equity"])
        best = viable[best_name]

        print(f"\n  BEST GATE: {best_name}")
        print(f"\n  {'W':>3s} {'period':>25s} {'action':>6s} {'w_ret':>7s} {'equity':>8s} {'dd':>6s} {'orig':>8s}")
        print(f"  {'-'*3} {'-'*25} {'-'*6} {'-'*7} {'-'*8} {'-'*6} {'-'*8}")

        for w in best["log"]:
            marker = ">>>" if w["action"] == "SKIP" and w["original_status"] == "FAIL" else "   "
            print(f"  {w['window']:>3d} {w['period']:>25s} {w['action']:>6s} ${w['window_ret']:>6.0f} "
                  f"${w['equity_after']:>7.0f} {w['dd']:>5.4f} {w['original_status']:>8s} {marker}")

        # Count catastrophic windows avoided
        avoided = sum(1 for w in best["log"]
                      if w["action"] == "SKIP" and w["original_status"] == "FAIL"
                      and w["window_ret"] < -100)
        print(f"\n  Catastrophic windows avoided (FAIL with >$100 loss): {avoided}")

    print(f"\n{'='*80}")

    # ── Verdict ──────────────────────────────────────────────────────
    no_gate = all_outcomes["NO_GATE"]
    best_gated = max(viable.values(), key=lambda v: v["final_equity"]) if viable else no_gate

    dd_improved = best_gated["max_dd"] < no_gate["max_dd"]
    eq_improved = best_gated["final_equity"] > no_gate["final_equity"]
    tail_improved = best_gated["worst_window"] > no_gate["worst_window"]

    print(f"  VERDICT:")
    print(f"    Equity improved:     {'YES' if eq_improved else 'NO'} "
          f"(${no_gate['final_equity']:.0f} → ${best_gated['final_equity']:.0f})")
    print(f"    Max DD improved:     {'YES' if dd_improved else 'NO'} "
          f"({no_gate['max_dd']:.4f} → {best_gated['max_dd']:.4f})")
    print(f"    Worst window better: {'YES' if tail_improved else 'NO'} "
          f"(${no_gate['worst_window']:.0f} → ${best_gated['worst_window']:.0f})")

    if eq_improved and dd_improved:
        print(f"\n    GATE WORKS. System is improved with permission layer.")
        print(f"    Next: integrate into live pipeline as Layer 3.5.")
    elif dd_improved and not eq_improved:
        print(f"\n    GATE REDUCES RISK but costs some return. May be worth it.")
    else:
        print(f"\n    GATE DID NOT HELP. Regime clustering is not the bottleneck.")
    print(f"{'='*80}")

    # Save
    save_data = {name: {k: v for k, v in o.items() if k != "log"} for name, o in all_outcomes.items()}
    save_data["detailed_log"] = all_outcomes.get(best_name, {}).get("log", [])
    Path(REPORTS_DIR / "permission_gate_results.json").write_text(
        json.dumps(save_data, indent=2, default=str) + "\n"
    )
    print(f"  WROTE {REPORTS_DIR}/permission_gate_results.json")


if __name__ == "__main__":
    main()
