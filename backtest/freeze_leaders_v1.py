import json
import math
from pathlib import Path

MIN_ABS_IC = 0.01
TOP_ASSETS = 6
LAGS = ("ic_1", "ic_2", "ic_4")

def _ok_num(x) -> bool:
    return isinstance(x, (int, float)) and (not math.isnan(x)) and (not math.isinf(x))

def main() -> None:
    in_path = Path("reports/cross_market_scores.json")
    if not in_path.exists():
        raise SystemExit("missing reports/cross_market_scores.json, run: PYTHONPATH=. python -m backtest.score_cross_market")

    rows = json.loads(in_path.read_text())

    ranked = []
    for r in rows:
        if r.get("error"):
            continue

        asset = r.get("asset")
        fs = r.get("feature_scores") or {}
        if not asset or not fs:
            continue

        best = None
        for feat, s in fs.items():
            if not isinstance(s, dict):
                continue

            n = s.get("n")
            stab = s.get("stability_std")
            if not _ok_num(stab):
                continue
            if not isinstance(n, int) or n < 2000:
                continue

            for lag in LAGS:
                ic = s.get(lag)
                if not _ok_num(ic):
                    continue

                abs_ic = abs(float(ic))
                if abs_ic < MIN_ABS_IC:
                    continue

                score = abs_ic / (float(stab) + 1e-9)
                cand = (score, abs_ic, float(ic), float(stab), int(n), asset, feat, lag)
                if best is None or cand[0] > best[0]:
                    best = cand

        if best is not None:
            ranked.append(best)

    ranked.sort(reverse=True)

    picked = []
    seen = set()
    for score, abs_ic, ic, stab, n, asset, feat, lag in ranked:
        if asset in seen:
            continue
        seen.add(asset)
        picked.append(asset)
        if len(picked) >= TOP_ASSETS:
            break

    out = {
        "version": "leaders_v1",
        "method": "max(abs(IC_lag))/stability_std with min abs(IC) and min n",
        "min_abs_ic": MIN_ABS_IC,
        "min_n": 2000,
        "picked_assets": picked,
        "ranking_table_top10": [
            {
                "asset": asset,
                "score": float(score),
                "abs_ic": float(abs_ic),
                "ic": float(ic),
                "stability_std": float(stab),
                "n": int(n),
                "feature": feat,
                "lag": lag,
            }
            for score, abs_ic, ic, stab, n, asset, feat, lag in ranked[:10]
        ],
    }

    Path("reports").mkdir(parents=True, exist_ok=True)
    Path("reports/leaders_v1.json").write_text(json.dumps(out, indent=2) + "\n")

    print("WROTE reports/leaders_v1.json")
    print("picked_assets", picked)
    if not picked:
        print("WARNING: picked_assets empty. That means every asset scored as error or had no valid IC values.")

if __name__ == "__main__":
    main()
