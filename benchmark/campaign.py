import argparse
import hashlib
import json
import os
import random
import sys
from datetime import datetime, timezone

import core

HOST = "127.0.0.1:8085"
PROJECT = "local-pubsub-u2"
SEED = 20261114
VERSION = 1

TIMINGS = {
    "warm": {"obs": 15, "pub": 2},
    "formal": {"obs": 75, "pub": 45},
    "validation": {"obs": 12, "pub": 0.3},
}


def guard():
    core.guard()


def _core_sha():
    import inspect
    src = inspect.getsourcefile(core)
    with open(src, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _campaign_sha():
    with open(__file__, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _utc():
    return datetime.now(timezone.utc).isoformat()


def build_plan(seed, validation=False):
    rng = random.Random(seed)
    plan = []
    if validation:
        rates = [50, 100]
        for i, rate in enumerate(rates):
            scen = ["A", "B", "C"]
            rng.shuffle(scen)
            for j, scenario in enumerate(scen):
                plan.append({
                    "id": "s%d-r%d-b0-%s" % (seed, rate, scenario),
                    "block_id": 0,
                    "scenario": scenario,
                    "rate_target": rate,
                    "order": i * 3 + j,
                    "kind": "integration",
                    "group": "validation",
                })
        return plan

    blocks = list(range(1, 11))
    groups = [(rate, blk) for rate in (50, 100) for blk in blocks]
    rng.shuffle(groups)
    order = 0
    for rate, blk in groups:
        scen = ["A", "B", "C"]
        rng.shuffle(scen)
        for scenario in scen:
            plan.append({
                "id": "s%d-r%d-b%d-%s" % (seed, rate, blk, scenario),
                "block_id": blk,
                "scenario": scenario,
                "rate_target": rate,
                "order": order,
                "kind": "formal",
                "group": "formal",
            })
            order += 1
    return plan


def _write_exclusive(path, data):
    with open(path, "x", encoding="utf-8") as fh:
        fh.write(data)


def _read_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _terminal(status):
    return status in ("completed", "failed", "interrupted")


def _load_manifest(path):
    latest = {}
    if not os.path.exists(path):
        return latest
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rid = rec.get("run_id")
            if rid:
                latest[rid] = rec
    return latest


def _append_manifest(fh, rec):
    fh.write(json.dumps(rec, sort_keys=True) + "\n")
    fh.flush()
    os.fsync(fh.fileno())


def _env_ok():
    host = os.environ.get("CAMPAIGN_HOST", HOST)
    project = os.environ.get("CAMPAIGN_PROJECT", PROJECT)
    if host != HOST or project != PROJECT:
        sys.stderr.write("guard: host/project mismatch\n")
        raise SystemExit(2)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--project", default=PROJECT)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--validation", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)

    if args.host != HOST:
        sys.stderr.write("guard: host hard-rejected\n")
        return 2
    if args.project != PROJECT:
        sys.stderr.write("guard: project hard-rejected\n")
        return 2

    guard()
    _env_ok()

    outdir = os.path.abspath(args.out)
    config_path = os.path.join(outdir, "config.json")
    plan_path = os.path.join(outdir, "plan.json")
    manifest_path = os.path.join(outdir, "manifest.jsonl")

    plan = build_plan(args.seed, validation=args.validation)
    kind = "validation" if args.validation else "formal"
    core_sha = _core_sha()
    campaign_sha = _campaign_sha()

    config = {
        "version": VERSION,
        "kind": kind,
        "seed": args.seed,
        "host": args.host,
        "project": args.project,
        "core_sha": core_sha,
        "campaign_sha": campaign_sha,
        "timings": TIMINGS,
    }

    if not args.resume:
        if os.path.exists(outdir):
            sys.stderr.write("refuse: output exists (use --resume)\n")
            return 3
        os.makedirs(outdir, exist_ok=False)
        _write_exclusive(config_path, json.dumps(config, indent=2, sort_keys=True))
        _write_exclusive(plan_path, json.dumps(plan, indent=2, sort_keys=True))
    else:
        if not os.path.isdir(outdir):
            sys.stderr.write("resume: output missing\n")
            return 3
        old_config = _read_json(config_path)
        old_plan = _read_json(plan_path)
        if old_config != config or old_plan != plan:
            sys.stderr.write("resume: config/plan mismatch\n")
            return 3

    if args.resume:
        latest = _load_manifest(manifest_path)
    else:
        latest = {}

    manifest = open(manifest_path, "a", encoding="utf-8")
    counts = {"completed": 0, "failed": 0, "interrupted": 0, "skipped": 0}
    current = None

    try:
        for item in plan:
            rid = item["id"]
            prev = latest.get(rid)
            if prev is not None:
                status = prev.get("status")
                if status == "started":
                    rec = dict(prev)
                    rec["status"] = "interrupted"
                    rec["updated_utc"] = _utc()
                    _append_manifest(manifest, rec)
                    sys.stdout.write("interrupted: %s\n" % rid)
                    sys.stdout.flush()
                    counts["interrupted"] += 1
                    continue
                if _terminal(status):
                    counts["skipped"] += 1
                    continue

            base = {
                "run_id": rid,
                "block_id": item["block_id"],
                "scenario": item["scenario"],
                "rate_target": item["rate_target"],
                "order": item["order"],
                "kind": item["kind"],
            }

            current = dict(base)
            started = dict(base)
            started["status"] = "started"
            started["started_utc"] = _utc()
            _append_manifest(manifest, started)
            sys.stdout.write("start: %s\n" % rid)
            sys.stdout.flush()

            run_dir = os.path.join(outdir, rid)
            try:
                os.makedirs(run_dir, exist_ok=False)
            except FileExistsError:
                sys.stderr.write("refuse: run dir exists %s\n" % rid)
                return 3

            current["phase"] = "warm" if kind == "formal" else ("formal" if kind == "formal" else "validation")
            if kind == "formal":
                warm = core.phase(
                    os.path.join(run_dir, "warm"), item["scenario"], item["rate_target"],
                    TIMINGS["warm"]["pub"], TIMINGS["warm"]["obs"],
                    rid + "-warm", "warm",
                )
                if warm.get("errors") or warm.get("critical") or warm.get("interrupted"):
                    rec = dict(base)
                    rec["status"] = "interrupted" if warm.get("interrupted") else "failed"
                    rec["phase"] = "warm"
                    rec["errors"] = warm.get("errors")
                    rec["critical"] = warm.get("critical")
                    rec["raw"] = warm.get("raw")
                    rec["meta"] = warm.get("meta")
                    rec["updated_utc"] = _utc()
                    _append_manifest(manifest, rec)
                    sys.stdout.write("%s: %s (warm)\n" % (rec["status"], rid))
                    sys.stdout.flush()
                    counts[rec["status"]] += 1
                    current = None
                    break
                sys.stdout.write("warm ok: %s\n" % rid)
                sys.stdout.flush()

            t = TIMINGS[kind]
            res = core.phase(
                os.path.join(run_dir, "formal" if kind == "formal" else "integration"),
                item["scenario"], item["rate_target"],
                t["pub"], t["obs"], rid, "formal" if kind == "formal" else "validation",
            )

            rec = dict(base)
            rec["errors"] = res.get("errors")
            rec["raw"] = res.get("raw")
            rec["meta"] = res.get("meta")
            rec["phase"] = "formal" if kind == "formal" else "validation"
            rec["valid"] = not (res.get("errors") or res.get("critical") or res.get("interrupted"))
            rec["updated_utc"] = _utc()

            if res.get("interrupted"):
                rec["status"] = "interrupted"
                counts["interrupted"] += 1
            elif not rec["valid"]:
                rec["status"] = "failed"
                counts["failed"] += 1
            else:
                rec["status"] = "completed"
                counts["completed"] += 1

            _append_manifest(manifest, rec)
            sys.stdout.write("%s: %s\n" % (rec["status"], rid))
            sys.stdout.flush()
            current = None

            if not rec["valid"] or rec["status"] != "completed":
                break
    except KeyboardInterrupt:
        sys.stdout.write("keyboard interrupt\n")
        sys.stdout.flush()
        if current is not None:
            rec = dict(current)
            rec["status"] = "interrupted"
            rec["updated_utc"] = _utc()
            _append_manifest(manifest, rec)
            current = None
        counts["interrupted"] += 1
    except Exception as exc:
        sys.stdout.write("unexpected: %r\n" % exc)
        sys.stdout.flush()
        if current is not None:
            rec = dict(current)
            rec["status"] = "failed"
            rec["error"] = repr(exc)
            rec["updated_utc"] = _utc()
            _append_manifest(manifest, rec)
            current = None
        counts["failed"] += 1
    finally:
        manifest.close()

    total = len(plan)
    sys.stdout.write(
        "end: total=%d completed=%d failed=%d interrupted=%d skipped=%d\n"
        % (total, counts["completed"], counts["failed"], counts["interrupted"], counts["skipped"])
    )
    sys.stdout.flush()

    if counts["failed"] or counts["interrupted"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
