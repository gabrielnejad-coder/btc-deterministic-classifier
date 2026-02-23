PROJECT — Adaptive 12-Hour BTC Direction Engine

Core Objective
Build a deterministic, cost-aware, multi-asset predictive system that:
1. Predicts BTC 12-hour forward directional movement
2. Uses frozen cross-market drivers
3. Adapts only through controlled rolling retraining
4. Converts probabilities into controlled trade decisions
5. Survives strict walkforward and robustness gates
6. Achieves backtest → paper → live parity
7. Is safe for consumer deployment

GLOBAL RULES
1. No lookahead anywhere
2. All features computed at bar close
3. Trades executed at next bar open
4. One position at a time
5. Hold_min_bars = 12
6. Stop loss = 2 percent
7. Fees and slippage always applied
8. Leaders frozen before test
9. No tuning on test data
10. Gate fail means go backward not forward
11. Horizon fixed at 12h unless full version reset
12. Position sizing must be defined and frozen
13. Leader selection methodology versioned and hashed
14. Drift metrics defined and monitored
15. Hard kill switch at 10 percent drawdown

POSITION SIZING V1
Position_size = 1.0
No leverage
No dynamic sizing

PHASES AND SECTIONS
Phase 1 Deterministic Foundation
Section 1 Config freeze
Section 2 Data integrity
Section 3 Backtest engine
Section 4 Baselines
Section 5 Anti cheating tests

Phase 2 Predictive Architecture
Section 6 Targets
Section 7 Features
Section 8 Leader freeze
Section 9 Probabilistic classifier
Section 10 Policy layer

Phase 3 Adaptation
Section 11 Rolling retrain
Section 12 Gates
Section 13 Robustness

Phase 4 Execution Parity
Section 14 Paper engine
Section 15 Production safety
Section 16 Live training wheels
