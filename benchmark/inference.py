import math
import statistics
from scipy.stats import binomtest

def paired_tests(metrics):
    METRICS = ["ratio_ack_rpc_per_umsg", "latency_p95_ms"]
    RATES = [50, 100]
    FAMILY_SIZE = 4
    ALPHA = 0.05

    keyed = {}
    for row in metrics:
        if not row.get("operational_valid"):
            continue
        key = (row["rate_target"], row["block_id"], row["scenario"])
        if key in keyed:
            raise ValueError("duplicate operational record: %r" % (key,))
        keyed[key] = row

    pairs = []
    tests = []

    for rate in RATES:
        for metric in METRICS:
            n_pairs = 0
            missing_blocks = []
            n_plus = n_minus = ties = 0
            diffs = []

            for block in range(1, 11):
                a = keyed.get((rate, block, "A"))
                c = keyed.get((rate, block, "C"))
                va = a.get(metric) if a else None
                vc = c.get(metric) if c else None
                if (
                    va is None or vc is None
                    or not math.isfinite(va) or not math.isfinite(vc)
                ):
                    missing_blocks.append(block)
                    continue

                d = va - vc
                n_pairs += 1
                diffs.append(d)
                if d > 0:
                    n_plus += 1
                elif d < 0:
                    n_minus += 1
                else:
                    ties += 1

                pairs.append({
                    "rate_target": rate,
                    "block_id": block,
                    "metric": metric,
                    "A": va,
                    "C": vc,
                    "difference": d,
                    "run_id_A": a["run_id"],
                    "run_id_C": c["run_id"],
                })

            n_eff = n_plus + n_minus
            if n_eff > 0:
                p_raw = binomtest(n_plus, n_eff, 0.5, alternative="two-sided").pvalue
            else:
                p_raw = None

            tests.append({
                "metric": metric,
                "rate_target": rate,
                "n_pairs": n_pairs,
                "n_eff": n_eff,
                "ties": ties,
                "n_plus": n_plus,
                "n_minus": n_minus,
                "mean_difference": statistics.mean(diffs) if diffs else None,
                "median_difference": statistics.median(diffs) if diffs else None,
                "p_raw": p_raw,
                "missing_blocks": missing_blocks,
            })

    finite = [t for t in tests if t["p_raw"] is not None]
    finite.sort(key=lambda t: t["p_raw"])
    prev = 0.0
    for rank, t in enumerate(finite):
        adjusted = max(prev, min(1.0, (FAMILY_SIZE - rank) * t["p_raw"]))
        prev = adjusted
        t["p_adjusted"] = adjusted
        t["reject"] = adjusted <= ALPHA

    for t in tests:
        if t["p_raw"] is None:
            t["p_adjusted"] = None
            t["reject"] = None
        t["family_size"] = FAMILY_SIZE
        t["alpha"] = ALPHA

    return pairs, tests
def main():
    import json
    import csv
    import sys
    import pathlib

    folder = pathlib.Path(sys.argv[1])
    metrics = json.loads((folder / "runs.json").read_text())

    pairs, tests = paired_tests(metrics)

    pair_fields = ["rate_target", "block_id", "metric", "A", "C", "difference", "run_id_A", "run_id_C"]
    with (folder / "comparacion_bloques.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=pair_fields)
        writer.writeheader()
        for row in pairs:
            writer.writerow(row)

    with (folder / "contrastes.json").open("w") as f:
        json.dump(tests, f, indent=2)

    test_fields = [
        "metric", "rate_target", "n_pairs", "n_eff", "ties", "n_plus", "n_minus",
        "mean_difference", "median_difference", "p_raw", "p_adjusted", "reject",
        "missing_blocks", "family_size", "alpha",
    ]
    with (folder / "contrastes.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=test_fields)
        writer.writeheader()
        for row in tests:
            out = dict(row)
            if isinstance(out.get("missing_blocks"), list):
                out["missing_blocks"] = json.dumps(out["missing_blocks"])
            for k, v in out.items():
                if v is None:
                    out[k] = ""
            writer.writerow(out)

    print(json.dumps(tests))


if __name__ == "__main__":
    main()

