import json, csv, sys
from pathlib import Path
from collections import OrderedDict
from normalize import normalize, FIELDS

root = Path(sys.argv[1])
out = Path(sys.argv[2])

cfg = json.loads((root / "config.json").read_text())
assert cfg.get("kind") == "formal", "config.kind must be 'formal'"
plan = json.loads((root / "plan.json").read_text())
assert isinstance(plan, list), "plan.json must be a list"

latest = {}
for line in (root / "manifest.jsonl").read_text().splitlines():
    if not line.strip():
        continue
    rec = json.loads(line)
    latest[rec["run_id"]] = rec

out.mkdir(parents=True, exist_ok=False)

metrics = []
with (out / "results.csv").open("w", newline="", encoding="utf-8") as fh:
    writer = csv.DictWriter(fh, fieldnames=FIELDS)
    writer.writeheader()
    for item in plan:
        rid = item["id"]
        status = latest.get(rid, {}).get("status", "pending")
        p = root / rid / "formal" / "raw.jsonl"
        if p.exists():
            kind = next(e for e in (json.loads(l) for l in p.read_text().splitlines() if l.strip()) if e["ev"] == "meta")["kind"]
            assert kind == "formal", f"run {rid}: kind must be 'formal', got {kind!r}"
            rows, m = normalize(p, item["block_id"], status)
            assert m["run_id"] == rid
            assert m["scenario"] == item["scenario"]
            writer.writerows(rows)
            metrics.append(m)
        else:
            metrics.append(dict(run_id=rid, block_id=item["block_id"], scenario=item["scenario"],
                                rate_target=item["rate_target"], status=status,
                                operational_valid=False, data_issue="noformaldata"))

(out / "runs.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

keys = []
for m in metrics:
    for k in m:
        if k not in keys:
            keys.append(k)
with (out / "fullmetrics.csv").open("w", newline="", encoding="utf-8") as fh:
    w = csv.DictWriter(fh, fieldnames=keys)
    w.writeheader()
    for m in metrics:
        row = OrderedDict()
        for k in keys:
            v = m.get(k, "")
            if k == "errors" and v is not None:
                v = json.dumps(v)
            if v is None:
                v = ""
            row[k] = v
        w.writerow(row)
