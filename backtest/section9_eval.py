import json

def load(p: str):
    with open(p, "r") as f:
        return json.load(f)

def main():
    v = load("reports/v2_test_metrics.json")
    b1 = load("reports/baseline_always_up_test_metrics.json")
    b2 = load("reports/baseline_yday_eq_today_test_metrics.json")

    gateA = v["max_drawdown"] <= 0.10
    gateB = (v["final_equity"] > b1["final_equity"]) and (v["final_equity"] > b2["final_equity"])

    print("SECTION 9 RESULTS\n")
    print("V2 test")
    print("final_equity", v["final_equity"])
    print("total_return", v["total_return"])
    print("max_drawdown", v["max_drawdown"])
    print("num_trades", v["num_trades"])
    print()
    print("Baseline always_up final_equity", b1["final_equity"])
    print("Baseline yday_eq_today final_equity", b2["final_equity"])
    print()
    print("Gate A drawdown <= 0.10", gateA)
    print("Gate B beats both baselines", gateB)
    print()
    print("DECISION", "PASS" if (gateA and gateB) else "FAIL")

if __name__ == "__main__":
    main()
