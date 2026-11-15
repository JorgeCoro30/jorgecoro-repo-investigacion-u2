import csv
import json
import math
import os
from datetime import datetime, timezone

FIELDS = [
    "run_id", "block_id", "scenario", "rate_target", "message_id",
    "delivery_attempt", "publish_timestamp", "receive_timestamp",
    "processing_time_ms", "ack_timestamp", "ack_rpc_id", "ack_ids_count",
    "redelivery", "status", "delivery_id", "app_id", "ack_id",
    "api_delivery_attempt", "publish_ns", "planned_ns", "receive_ns",
    "process_start_ns", "done_ns", "ack_start_ns", "ack_end_ns", "ack_code",
    "in_window", "completed_in_window", "accepted_known", "generator_lag_ms",
]


def _iso(anchor_utc_ns, anchor_perf_ns, ns):
    if ns is None:
        return None
    offset = ns - anchor_perf_ns
    epoch_ns = anchor_utc_ns + offset
    sec, rem = divmod(epoch_ns, 1_000_000_000)
    dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".%09dZ" % rem


def _pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = math.ceil(p * len(s))
    if k < 1:
        k = 1
    return s[k - 1]


def _median(values):
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    if n % 2:
        return float(s[n // 2])
    return (s[n // 2 - 1] + s[n // 2]) / 2.0


def _in(x, lo, hi):
    if x is None or lo is None or hi is None:
        return False
    return lo <= x < hi


def normalize(path, block_id, status="completed"):
    meta = {}
    attempts = {}
    results = {}
    misses = {}
    receipts = []
    processes = []
    proc_by_did = {}
    batches = {}
    cancelled = set()
    rpcs = {}
    rpc_starts = []
    rpc_ends = {}
    cleanup = []
    errors = []
    ack_pending = []

    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            ns = ev.get("ev")
            if ns == "meta":
                meta = ev
            elif ns == "pub_attempt":
                attempts[ev["app_id"]] = {
                    "idx": ev.get("idx"),
                    "planned_ns": ev.get("planned_ns"),
                    "actual_ns": ev.get("actual_ns"),
                    "accepted": None,
                    "message_id": None,
                    "end_ns": None,
                    "error": None,
                }
            elif ns == "pub_result":
                attempts.setdefault(ev["app_id"], {
                    "idx": None, "planned_ns": None, "actual_ns": None,
                })
                a = attempts[ev["app_id"]]
                a["accepted"] = bool(ev.get("accepted"))
                a["message_id"] = ev.get("message_id")
                a["end_ns"] = ev.get("end_ns")
                a["error"] = ev.get("error")
                if a["accepted"] and a["message_id"] is not None:
                    results[a["message_id"]] = ev["app_id"]
            elif ns == "miss":
                misses[ev["idx"]] = ev.get("planned_ns")
            elif ns == "receipt":
                receipts.append(ev)
            elif ns == "process":
                processes.append(ev)
                proc_by_did[ev["delivery_id"]] = ev
            elif ns == "batch_intent":
                batches[ev["batch_id"]] = {
                    "delivery_ids": list(ev.get("delivery_ids") or []),
                    "ack_ids": list(ev.get("ack_ids") or []),
                    "send_ns": ev.get("send_ns"),
                    "wait_by_id_ns": ev.get("wait_by_id_ns") or {},
                    "ack_rpc_id": None,
                }
            elif ns == "batch_cancelled":
                cancelled.add(ev["batch_id"])
            elif ns == "rpc_start":
                rpcs[ev["rpc_id"]] = {
                    "method": ev.get("method"),
                    "ack_ids": list(ev.get("ack_ids") or []),
                    "start_ns": ev.get("start_ns"),
                }
                rpc_starts.append((ev.get("start_ns"), ev["rpc_id"]))
            elif ns == "rpc_end":
                rpc_ends[ev["rpc_id"]] = {
                    "code": ev.get("code"),
                    "end_ns": ev.get("end_ns"),
                }
            elif ns in ("cleanup", "cleanup_subscription", "cleanup_topic"):
                cleanup.append(ev)
            elif ns == "error":
                errors.append(ev)
            elif ns == "ack_pending":
                ack_pending.append(ev)

    rpc_starts.sort(key=lambda t: (t[0] if t[0] is not None else 0, t[1]))

    ack_rpc_candidates = {}
    for st, rid in rpc_starts:
        r = rpcs.get(rid)
        if r is None:
            continue
        m = r.get("method") or ""
        if not m.endswith("/Acknowledge"):
            continue
        key = tuple(r.get("ack_ids") or [])
        ack_rpc_candidates.setdefault(key, []).append((st, rid))

    used = set()
    for bid, b in batches.items():
        if bid in cancelled:
            continue
        send_ns = b["send_ns"]
        if send_ns is None:
            continue
        cands = ack_rpc_candidates.get(tuple(b["ack_ids"]))
        if not cands:
            continue
        for st, rid in cands:
            if st is None or st < send_ns or rid in used:
                continue
            b["ack_rpc_id"] = rid
            used.add(rid)
            break

    rid_to_batch = {}
    for bid, b in batches.items():
        if b.get("ack_rpc_id"):
            rid_to_batch[b["ack_rpc_id"]] = bid

    N_modify = 0
    for r in rpcs.values():
        m = r.get("method") or ""
        if m.endswith("/ModifyAckDeadline"):
            N_modify += 1

    did_to_ack = {}
    did_to_batch = {}
    for bid, b in batches.items():
        for did in b["delivery_ids"]:
            did_to_batch[did] = bid

    if not meta:
        raise ValueError("missingmetadata")
    run_id = meta.get("run_id")
    scenario = meta.get("scenario")
    rate = meta.get("rate")
    t0 = meta.get("t0_ns")
    obs_s = meta.get("obs_s")
    pub_end = meta.get("pub_end_ns")
    cut = meta.get("cut_ns")
    ap_ns = meta.get("anchor_perf_ns")
    au_ns = meta.get("anchor_utc_ns")

    def iso(ns):
        return _iso(au_ns, ap_ns, ns)

    rows = []
    recv_in = []
    receipt_app_ids = set()
    receipt_by_did = {}
    for r in receipts:
        if r.get("app_id") is not None:
            receipt_app_ids.add(r.get("app_id"))
        if r.get("delivery_id") is not None:
            receipt_by_did[r.get("delivery_id")] = r
    accepted_msg_ids = set(v for v, a in results.items())
    known_msgs = set(attempts[k]["message_id"] for k in attempts
                     if attempts[k].get("accepted") and attempts[k].get("message_id") is not None)

    for r in receipts:
        did = r.get("delivery_id")
        app_id = r.get("app_id")
        recv_ns = r.get("recv_ns")
        bid = did_to_batch.get(did)
        batch = batches.get(bid) if bid else None
        ack_rpc_id = batch.get("ack_rpc_id") if batch else None
        rpc = rpcs.get(ack_rpc_id) if ack_rpc_id else None
        ack_start = rpc.get("start_ns") if rpc else None
        rpc_end_info = rpc_ends.get(ack_rpc_id) if ack_rpc_id else None
        ack_end = rpc_end_info.get("end_ns") if rpc_end_info else None
        ack_code = rpc_end_info.get("code") if rpc_end_info else None
        local_attempt = r.get("local_attempt")
        local_attempt = int(local_attempt) if local_attempt is not None else None

        proc = proc_by_did.get(did)
        process_start = proc.get("start_ns") if proc else None
        done_ns = proc.get("done_ns") if proc else None
        proc_ms = proc.get("proc_ms") if proc else None
        late_proc = proc.get("late") if proc else None

        att = attempts.get(app_id) if app_id is not None else None
        actual_ns = att.get("actual_ns") if att else None
        planned_ns = att.get("planned_ns") if att else None
        message_id = r.get("message_id")
        if message_id is None and att:
            message_id = att.get("message_id")

        in_window = _in(recv_ns, t0, cut) if (t0 is not None and cut is not None) else None
        completed_in_window = None
        if done_ns is not None and t0 is not None and cut is not None and actual_ns is not None:
            completed_in_window = _in(done_ns, t0, cut) and _in(actual_ns, t0, pub_end)

        if not in_window:
            row_status = "late_receipt"
        elif proc is None:
            row_status = "unprocessed"
        elif late_proc:
            row_status = "late_process"
        else:
            row_status = "processed"

        accepted_known = None
        if message_id is not None:
            accepted_known = message_id in accepted_msg_ids

        gen_lag = None
        if actual_ns is not None and planned_ns is not None:
            gen_lag = (actual_ns - planned_ns) / 1e6

        redelivery = bool(local_attempt is not None and local_attempt > 1)
        ack_ids_count = len(batch["ack_ids"]) if ack_rpc_id is not None else None

        rows.append({
            "run_id": run_id,
            "block_id": block_id,
            "scenario": scenario,
            "rate_target": rate,
            "message_id": message_id,
            "delivery_attempt": local_attempt,
            "publish_timestamp": iso(actual_ns),
            "receive_timestamp": iso(recv_ns),
            "processing_time_ms": proc_ms,
            "ack_timestamp": iso(ack_start),
            "ack_rpc_id": ack_rpc_id,
            "ack_ids_count": ack_ids_count,
            "redelivery": redelivery,
            "status": row_status,
            "delivery_id": did,
            "app_id": app_id,
            "ack_id": r.get("ack_id"),
            "api_delivery_attempt": r.get("api_delivery_attempt"),
            "publish_ns": actual_ns,
            "planned_ns": planned_ns,
            "receive_ns": recv_ns,
            "process_start_ns": process_start,
            "done_ns": done_ns,
            "ack_start_ns": ack_start,
            "ack_end_ns": ack_end,
            "ack_code": ack_code,
            "in_window": in_window,
            "completed_in_window": completed_in_window,
            "accepted_known": accepted_known,
            "generator_lag_ms": gen_lag,
        })

    delivered_msg_ids = set()
    for r in receipts:
        mid = r.get("message_id")
        if mid is None:
            app_id = r.get("app_id")
            if app_id in attempts:
                mid = attempts[app_id].get("message_id")
        if mid is not None:
            delivered_msg_ids.add(mid)

    for app_id, att in attempts.items():
        if app_id in receipt_app_ids:
            continue
        mid = att.get("message_id")
        if mid is not None and mid in delivered_msg_ids:
            continue
        planned_ns = att.get("planned_ns")
        actual_ns = att.get("actual_ns")
        accepted = bool(att.get("accepted"))
        rows.append({
            "run_id": run_id,
            "block_id": block_id,
            "scenario": scenario,
            "rate_target": rate,
            "message_id": mid,
            "delivery_attempt": None,
            "publish_timestamp": iso(actual_ns),
            "receive_timestamp": None,
            "processing_time_ms": None,
            "ack_timestamp": None,
            "ack_rpc_id": None,
            "ack_ids_count": None,
            "redelivery": None,
            "status": "unprocessed" if accepted else "ambiguous",
            "delivery_id": None,
            "app_id": app_id,
            "ack_id": None,
            "api_delivery_attempt": None,
            "publish_ns": actual_ns,
            "planned_ns": planned_ns,
            "receive_ns": None,
            "process_start_ns": None,
            "done_ns": None,
            "ack_start_ns": None,
            "ack_end_ns": None,
            "ack_code": None,
            "in_window": None,
            "completed_in_window": None,
            "accepted_known": accepted,
            "generator_lag_ms": (actual_ns - planned_ns) / 1e6 if (actual_ns is not None and planned_ns is not None) else None,
        })

    for idx, planned_ns in misses.items():
        rows.append({
            "run_id": run_id,
            "block_id": block_id,
            "scenario": scenario,
            "rate_target": rate,
            "message_id": None,
            "delivery_attempt": None,
            "publish_timestamp": None,
            "receive_timestamp": None,
            "processing_time_ms": None,
            "ack_timestamp": None,
            "ack_rpc_id": None,
            "ack_ids_count": None,
            "redelivery": None,
            "status": "miss",
            "delivery_id": None,
            "app_id": None,
            "ack_id": None,
            "api_delivery_attempt": None,
            "publish_ns": None,
            "planned_ns": planned_ns,
            "receive_ns": None,
            "process_start_ns": None,
            "done_ns": None,
            "ack_start_ns": None,
            "ack_end_ns": None,
            "ack_code": None,
            "in_window": None,
            "completed_in_window": None,
            "accepted_known": None,
            "generator_lag_ms": None,
        })

    in_window_receipts = [r for r in receipts
                          if _in(r.get("recv_ns"), t0, cut) if t0 is not None and cut is not None]
    if t0 is None or cut is None:
        in_window_receipts = []

    U_msg_set = set()
    for r in receipts:
        if not _in(r.get("recv_ns"), t0, cut):
            continue
        mid = r.get("message_id")
        if mid is None:
            app_id = r.get("app_id")
            if app_id in attempts:
                mid = attempts[app_id].get("message_id")
        if mid is not None:
            U_msg_set.add(mid)
    U_msg = len(U_msg_set)
    M = len(in_window_receipts)

    first_done_by_app = {}
    for p in processes:
        did = p.get("delivery_id")
        r = receipt_by_did.get(did)
        if r is None:
            continue
        app_id = r.get("app_id")
        att = attempts.get(app_id)
        actual_ns = att.get("actual_ns") if att else None
        done_ns = p.get("done_ns")
        if (app_id is not None and done_ns is not None and actual_ns is not None
                and _in(done_ns, t0, cut) and _in(actual_ns, t0, pub_end)):
            if app_id not in first_done_by_app or done_ns < first_done_by_app[app_id]:
                first_done_by_app[app_id] = done_ns

    F = set(first_done_by_app.keys())

    known_accepted = set()
    for app_id, att in attempts.items():
        if att.get("accepted") and att.get("message_id") is not None:
            actual_ns = att.get("actual_ns")
            if actual_ns is not None and t0 is not None and pub_end is not None and _in(actual_ns, t0, pub_end):
                known_accepted.add(app_id)
    K = len(known_accepted)

    latencies = []
    for app_id, done_ns in first_done_by_app.items():
        att = attempts.get(app_id)
        actual_ns = att.get("actual_ns") if att else None
        if actual_ns is not None:
            latencies.append((done_ns - actual_ns) / 1e6)

    N_rpc = 0
    N_ok = 0
    for rid, r in rpcs.items():
        m = r.get("method") or ""
        if not m.endswith("/Acknowledge"):
            continue
        start = r.get("start_ns")
        end_info = rpc_ends.get(rid)
        end = end_info.get("end_ns") if end_info else None
        if start is None or end is None:
            continue
        if t0 is not None and cut is not None and start >= t0 and end < cut:
            N_rpc += 1
            if end_info.get("code") == "OK":
                N_ok += 1

    gen_lags = []
    for att in attempts.values():
        actual_ns = att.get("actual_ns")
        planned_ns = att.get("planned_ns")
        if actual_ns is not None and planned_ns is not None:
            gen_lags.append((actual_ns - planned_ns) / 1e6)

    proc_ms_in_window = []
    for p in processes:
        did = p.get("delivery_id")
        r = receipt_by_did.get(did)
        recv_ns = r.get("recv_ns") if r else None
        if _in(recv_ns, t0, cut) and p.get("proc_ms") is not None:
            proc_ms_in_window.append(p.get("proc_ms"))

    pub_s = None
    if pub_end is not None and t0 is not None:
        pub_s = (pub_end - t0) / 1e9
    pub_window_attempts = 0
    for att in attempts.values():
        actual_ns = att.get("actual_ns")
        if actual_ns is not None and t0 is not None and pub_end is not None and _in(actual_ns, t0, pub_end):
            pub_window_attempts += 1

    latency_p50 = _pct(latencies, 0.50)
    latency_p95 = _pct(latencies, 0.95)
    gen_lag_p95 = _pct(gen_lags, 0.95)
    proc_median = _median(proc_ms_in_window)

    is_b = (scenario == "B") if scenario else False
    ratio_original = (N_rpc / U_msg) if U_msg > 0 else None
    ratio_extra = (N_rpc / len(F)) if len(F) > 0 else None
    if is_b:
        ratio_original = None
        ratio_extra = None

    goodput = (len(F) / obs_s) if (obs_s and obs_s > 0) else None
    amplification = (M / U_msg) if U_msg > 0 else None
    redelivery = (M - U_msg) if M >= U_msg else None
    fraction = (redelivery / M) if (redelivery is not None and M > 0) else None
    offered_rate = (pub_window_attempts / pub_s) if (pub_s and pub_s > 0) else None
    accepted_rate = (K / pub_s) if (pub_s and pub_s > 0) else None

    f_and_k = F & known_accepted
    coverage = (len(f_and_k) / K) if K > 0 else None
    censored = (len(known_accepted - F) / K) if K > 0 else None
    pending = len(known_accepted - F)
    ambiguous_completed = len(F - known_accepted)

    lat_thresh = (100.0 / rate) if rate else None
    pending_ack_cut = 0
    if is_b:
        pending_ack_cut = 0
    else:
        for ev in ack_pending:
            pending_ack_cut = (ev.get("pending") or 0) + (ev.get("qsize") or 0)

    qualify_condition = (
        coverage is not None and coverage >= 0.95
        and censored is not None and censored <= 0.05
        and gen_lag_p95 is not None and lat_thresh is not None and gen_lag_p95 <= lat_thresh
        and proc_median is not None and 4.5 <= proc_median <= 5.5
        and pending == 0
        and len(misses) == 0
        and len(errors) == 0
        and pending_ack_cut == 0
    )

    if is_b:
        qualify = None
    else:
        qualify = qualify_condition

    late_receipts = 0
    late_process = 0
    for r in receipts:
        if not _in(r.get("recv_ns"), t0, cut):
            late_receipts += 1
        else:
            proc = proc_by_did.get(r.get("delivery_id"))
            if proc and proc.get("late"):
                late_process += 1

    late_ack_rpcs = 0
    for rid, r in rpcs.items():
        m = r.get("method") or ""
        if not m.endswith("/Acknowledge"):
            continue
        end_info = rpc_ends.get(rid)
        if end_info and t0 is not None and cut is not None:
            end = end_info.get("end_ns")
            if end is not None and end >= cut:
                late_ack_rpcs += 1

    cleanup_failed = any(ev.get("failed") or ev.get("ok") is False for ev in cleanup)
    operational_valid = (status == "completed") and not errors and not cleanup_failed

    error_counts = {}
    for ev in errors:
        key = ev.get("kind") or ev.get("type") or ev.get("message") or "unknown"
        error_counts[key] = error_counts.get(key, 0) + 1

    total_accepted = sum(1 for a in attempts.values() if a.get("accepted"))

    metrics = {
        "run_id": run_id,
        "block_id": block_id,
        "scenario": scenario,
        "rate_target": rate,
        "status": status,
        "operational_valid": operational_valid,
        "U_msg": U_msg,
        "M_deliveries": M,
        "F_completed": len(F),
        "K_known_accepted": K,
        "latency_p50_ms": latency_p50,
        "latency_p95_ms": latency_p95,
        "N_ack_rpc": N_rpc,
        "N_ack_ok": N_ok,
        "ratio_ack_rpc_per_umsg": ratio_original,
        "ratio_ack_rpc_per_ok": ratio_extra,
        "goodput_per_s": goodput,
        "amplification": amplification,
        "redelivery_count": redelivery,
        "redelivery_fraction": fraction,
        "offered_rate": offered_rate,
        "accepted_rate": accepted_rate,
        "coverage": coverage,
        "censored": censored,
        "pending": pending,
        "ambiguous_completed": ambiguous_completed,
        "generator_lag_p95_ms": gen_lag_p95,
        "proc_median_ms": proc_median,
        "qualify": qualify,
        "N_modify": N_modify,
        "late_receipts": late_receipts,
        "late_process": late_process,
        "late_ack_rpcs": late_ack_rpcs,
        "errors": error_counts,
        "n_errors": len(errors),
        "misses": len(misses),
        "accepted": total_accepted,
        "attempts": len(attempts),
        "totalrows": len(rows),
        "pending_ack_cut": pending_ack_cut,
    }

    return rows, metrics


def write_csv(path, rows, fields=FIELDS):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


if __name__ == "__main__":
    import sys
    src = sys.argv[1]
    bid = sys.argv[2] if len(sys.argv) > 2 else os.path.basename(src)
    rows, metrics = normalize(src, bid)
    write_csv(os.path.splitext(src)[0] + ".csv", rows)
    print(json.dumps(metrics, indent=2, default=str))
