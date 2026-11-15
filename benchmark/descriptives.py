import sys, os, json
import pandas as pd
import numpy as np

COUNTS = ["U_msg","M_deliveries","F_completed","K_known_accepted","N_ack_rpc","N_ack_ok","N_modify",
          "late_receipts","late_process","late_ack_rpcs","n_errors","misses","accepted","attempts",
          "totalrows","pending_ack_cut"]
LATE = ["late_receipts","late_process","late_ack_rpcs"]
IDCOLS = ["run_id","block_id","scenario","rate_target","status","operational_valid"]
METRICS = ["ratio_ack_rpc_per_umsg","ratio_ack_rpc_per_ok","F_completed","goodput_per_s","amplification",
           "latency_p95_ms","redelivery_fraction","pending","offered_rate","accepted_rate","censored",
           "generator_lag_p95_ms"]

def main():
    folder = sys.argv[1]
    path = os.path.join(folder, "runs.json")
    with open(path, "r", encoding="utf-8") as f:
        runs = json.load(f)
    df = pd.DataFrame(runs)
    for c in IDCOLS:
        if c not in df.columns:
            df[c] = pd.NA
    for c in COUNTS:
        if c not in df.columns:
            df[c] = pd.NA

    valid = df[df["operational_valid"] == True].copy()

    sc = df["status"].value_counts(dropna=False).to_dict()
    summary_counts = {
        "planned": int(len(df)),
        "completed": int((df["status"] == "completed").sum()),
        "failed": int((df["status"] == "failed").sum()),
        "interrupted": int((df["status"] == "interrupted").sum()),
        "started": int((df["status"] == "started").sum()),
        "pending": int((df["status"] == "pending").sum()),
        "operational_valid_sum": int((df["operational_valid"] == True).sum()),
        "status_counts": sc,
        "opvalid_counts": df["operational_valid"].value_counts(dropna=False).to_dict(),
    }
    with open(os.path.join(folder, "summary_counts.json"), "w", encoding="utf-8") as f:
        json.dump(summary_counts, f, indent=2)

    comp_cols = [c for c in IDCOLS if c in df.columns] + COUNTS
    df.reindex(columns=comp_cols).to_csv(os.path.join(folder, "runs.csv"), index=False)

    others = [c for c in df.columns if c not in COUNTS and c not in LATE and c != "errors" and c not in IDCOLS]
    ind_cols = [c for c in IDCOLS if c in df.columns] + others
    df.reindex(columns=ind_cols).to_csv(os.path.join(folder, "indicadores.csv"), index=False)

    inc_rows = []
    for _, r in df.iterrows():
        nerr = r.get("n_errors")
        flagged = (r.get("status") != "completed") or (pd.notna(nerr) and nerr > 0)
        latevals = [r.get(c) for c in LATE]
        if all(pd.notna(v) for v in latevals):
            latecount = int(sum(latevals))
        else:
            latecount = None
        qual = r.get("qualify")
        inc_rows.append({
            "run_id": r.get("run_id"), "block_id": r.get("block_id"),
            "scenario": r.get("scenario"), "rate_target": r.get("rate_target"),
            "status": r.get("status"), "operational_valid": r.get("operational_valid"),
            "n_errors": nerr, "errors": r.get("errors"),
            "qualify": qual, "incidence_flag": bool(flagged),
            "degraded": (qual is False), "late_count": latecount,
            "pending": r.get("pending"), "censored": r.get("censored"),
            "generator_lag_p95_ms": r.get("generator_lag_p95_ms"),
            "pending_ack_cut": r.get("pending_ack_cut"),
        })
    pd.DataFrame(inc_rows).to_csv(os.path.join(folder, "incidencias.csv"), index=False)

    desc_rows = []
    for (scen, rate), g in valid.groupby(["scenario", "rate_target"], dropna=False):
        for m in METRICS:
            if m not in g.columns:
                continue
            s = pd.to_numeric(g[m], errors="coerce")
            nn = s.dropna()
            desc_rows.append({
                "scenario": scen, "rate_target": rate, "metric": m,
                "n": int(nn.count()), "mean": nn.mean() if len(nn) else pd.NA,
                "sd": nn.std(ddof=1) if len(nn) > 1 else pd.NA,
                "median": nn.median() if len(nn) else pd.NA,
                "q1": nn.quantile(0.25) if len(nn) else pd.NA,
                "q3": nn.quantile(0.75) if len(nn) else pd.NA,
                "min": nn.min() if len(nn) else pd.NA,
                "max": nn.max() if len(nn) else pd.NA,
                "nmissing": int(s.isna().sum()),
            })
    pd.DataFrame(desc_rows, columns=["scenario","rate_target","metric","n","mean","sd","median","q1","q3","min","max","nmissing"]).to_csv(
        os.path.join(folder, "descriptivos.csv"), index=False)

    out_rows = []
    for (scen, rate), g in valid.groupby(["scenario", "rate_target"], dropna=False):
        for m in METRICS:
            if m not in g.columns:
                continue
            s = pd.to_numeric(g[m], errors="coerce").dropna()
            if len(s) < 4:
                continue
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr = q3 - q1
            lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            sub = g.assign(_v=pd.to_numeric(g[m], errors="coerce"))
            for _, r in sub.iterrows():
                if pd.notna(r["_v"]) and (r["_v"] < lo or r["_v"] > hi):
                    out_rows.append({"run_id": r.get("run_id"), "scenario": scen, "rate_target": rate,
                                     "metric": m, "value": r["_v"], "lower": lo, "upper": hi})
    pd.DataFrame(out_rows, columns=["run_id","scenario","rate_target","metric","value","lower","upper"]).to_csv(
        os.path.join(folder, "atipicos.csv"), index=False)

if __name__ == "__main__":
    main()
