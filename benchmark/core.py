import os
import json
import uuid
import time
import threading
import hashlib
import math
import queue
import pathlib
import grpc

from google.api_core.exceptions import DeadlineExceeded
from google.pubsub_v1.services.publisher import PublisherClient
from google.pubsub_v1.services.publisher.transports import PublisherGrpcTransport
from google.pubsub_v1.services.subscriber import SubscriberClient
from google.pubsub_v1.services.subscriber.transports import SubscriberGrpcTransport

HOST = "127.0.0.1:8085"
PROJECT = "local-pubsub-u2"


def now():
    return time.perf_counter_ns()


class Logger:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self._fh = open(path, "a", encoding="utf-8", buffering=1)
        self.errors = []

    def log(self, ev, **fields):
        rec = {"ev": ev, "ns": now(), **fields}
        line = json.dumps(rec, separators=(",", ":"), ensure_ascii=True)
        with self._lock:
            try:
                self._fh.write(line + "\n")
                self._fh.flush()
            except Exception as exc:
                self.errors.append(repr(exc))
                raise

    def close(self):
        with self._lock:
            try:
                self._fh.flush()
                os.fsync(self._fh.fileno())
            finally:
                self._fh.close()


class Interceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, logger):
        self.logger = logger

    def intercept_unary_unary(self, continuation, client_call_details, request):
        rpc_id = str(uuid.uuid4())
        method = getattr(client_call_details, "method", None)
        ack_ids = list(getattr(request, "ack_ids", []))
        start_ns = now()
        self.logger.log("rpc_start", rpc_id=rpc_id, method=method, ack_ids=ack_ids, start_ns=start_ns)
        try:
            call = continuation(client_call_details, request)
        except Exception:
            self.logger.log("rpc_end", rpc_id=rpc_id, method=method, end_ns=now(), code="CONTINUATION_ERROR")
            raise

        def _done(_call):
            try:
                code = _call.code().name
                self.logger.log("rpc_end", rpc_id=rpc_id, method=method, end_ns=now(), code=code)
            except Exception as exc:
                self.logger.errors.append(repr(exc))

        call.add_done_callback(_done)
        return call


def clients(logger):
    base = grpc.insecure_channel(HOST)
    channel = grpc.intercept_channel(base, Interceptor(logger))
    pub = PublisherClient(transport=PublisherGrpcTransport(channel=channel))
    sub = SubscriberClient(transport=SubscriberGrpcTransport(channel=channel))
    return base, pub, sub


def guard():
    if os.environ.get("PUBSUB_EMULATOR_HOST") != HOST:
        raise RuntimeError("PUBSUB_EMULATOR_HOST must exactly match HOST")


def sourcehash():
    return hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
def workers(log, pub, sub, topic, subscription, scenario, rate, pub_s, t0, pub_end, cut, stop, errors, q):
    def producer():
        try:
            n = math.ceil(rate * pub_s)
            for i in range(n):
                planned_ns = t0 + int(i * 1e9 / rate)
                while True:
                    remaining = (planned_ns - now()) / 1e9
                    if remaining <= 0:
                        break
                    time.sleep(min(remaining, 0.002))
                actual_ns = now()
                if actual_ns >= pub_end or stop.is_set():
                    for j in range(i, n):
                        log.log("miss", idx=j, planned_ns=t0 + int(j * 1e9 / rate))
                    break
                app_id = str(uuid.uuid4())
                log.log("pub_attempt", idx=i, app_id=app_id, planned_ns=planned_ns, actual_ns=actual_ns)
                try:
                    resp = pub.publish(
                        request={
                            "topic": topic,
                            "messages": [
                                {"data": b"x" * 4096, "attributes": {"app_id": app_id}},
                            ],
                        },
                        retry=None,
                        timeout=1,
                    )
                    message_ids = resp.message_ids
                    if len(message_ids) != 1:
                        raise RuntimeError(f"expected exactly one message_id, got {len(message_ids)}")
                    message_id = message_ids[0]
                    log.log("pub_result", app_id=app_id, message_id=message_id, end_ns=now(), accepted=True)
                except Exception as exc:
                    log.log("pub_result", app_id=app_id, end_ns=now(), accepted=False, error=repr(exc))
                    errors.append(repr(exc))
                    stop.set()
                    continue
        except Exception as exc:
            errors.append(repr(exc))
            log.log("error", where="producer", error=repr(exc))
            stop.set()

    def receiver():
        attempts = {}
        try:
            while now() < cut and not stop.is_set():
                try:
                    resp = sub.pull(
                        request={"subscription": subscription, "max_messages": 8},
                        retry=None,
                        timeout=0.5,
                    )
                except DeadlineExceeded:
                    continue
                except Exception as exc:
                    errors.append(repr(exc))
                    log.log("error", where="receiver_pull", error=repr(exc))
                    stop.set()
                    break

                received_messages = getattr(resp, "received_messages", [])
                if not received_messages:
                    time.sleep(0.001)
                    continue
                rns = now()
                for rm in received_messages:
                    mid = rm.message.message_id
                    app_id = rm.message.attributes.get("app_id")
                    delivery_id = str(uuid.uuid4())
                    attempts[mid] = attempts.get(mid, 0) + 1
                    log.log(
                        "receipt",
                        delivery_id=delivery_id,
                        message_id=mid,
                        app_id=app_id,
                        ack_id=rm.ack_id,
                        local_attempt=attempts[mid],
                        api_delivery_attempt=rm.delivery_attempt,
                        recv_ns=rns,
                        datasize=len(rm.message.data),
                    )
                    start = now()
                    if rns >= cut or start >= cut:
                        continue
                    end_limit = start + 5_000_000
                    data = bytes(rm.message.data)
                    while now() < end_limit:
                        hashlib.sha256(data).digest()
                    done = now()
                    proc_ms = (done - start) / 1e6
                    log.log(
                        "process",
                        delivery_id=delivery_id,
                        start_ns=start,
                        done_ns=done,
                        proc_ms=proc_ms,
                        late=done >= cut,
                    )
                    if scenario in ("A", "C") and done < cut:
                        try:
                            q.put_nowait((delivery_id, rm.ack_id, done))
                        except Exception as exc:
                            errors.append(repr(exc))
                            log.log("error", where="receiver_queue", error=repr(exc))
                            stop.set()
                            return
        except Exception as exc:
            errors.append(repr(exc))
            log.log("error", where="receiver", error=repr(exc))
            stop.set()

    return producer, receiver

def ack_worker(log, sub, subscription, scenario, cut, stop, errors, q):
    if scenario == "B":
        raise RuntimeError("ack_worker is only used for scenarios A and C")

    k = 1 if scenario == "A" else 8
    tau_ns = 0 if scenario == "A" else 50_000_000
    buf = []

    try:
        while now() < cut and not stop.is_set():
            if buf and (len(buf) >= k or now() >= buf[0][2] + tau_ns):
                send_ns = now()
                if send_ns >= cut:
                    return
                batch_id = str(uuid.uuid4())
                delivery_ids = [item[0] for item in buf]
                ack_ids = [item[1] for item in buf]
                wait_by_id_ns = {item[1]: send_ns - item[2] for item in buf}
                log.log(
                    "batch_intent",
                    batch_id=batch_id,
                    delivery_ids=delivery_ids,
                    ack_ids=ack_ids,
                    send_ns=send_ns,
                    wait_by_id_ns=wait_by_id_ns,
                )
                if now() >= cut:
                    log.log("batch_cancelled", batch_id=batch_id, send_ns=now())
                    return
                try:
                    sub.acknowledge(
                        request={"subscription": subscription, "ack_ids": ack_ids},
                        retry=None,
                        timeout=1,
                    )
                    log.log("batch_result", batch_id=batch_id, end_ns=now(), ok=True)
                except Exception as exc:
                    errors.append(repr(exc))
                    log.log("error", where="ack_worker", error=repr(exc))
                    stop.set()
                    break
                finally:
                    for _ in buf:
                        q.task_done()
                    buf.clear()
                continue

            oldest_done = buf[0][2] if buf else now()
            timeout_s = max(
                0.0,
                min(
                    0.05,
                    (cut - now()) / 1e9,
                    ((oldest_done + tau_ns - now()) / 1e9) if buf else 0.05,
                ),
            )
            try:
                item = q.get(timeout=timeout_s)
            except queue.Empty:
                continue
            buf.append(item)
    except Exception as exc:
        errors.append(repr(exc))
        log.log("error", where="ack_worker_outer", error=repr(exc))
        stop.set()
    finally:
        log.log("ack_pending", pending=len(buf), qsize=q.qsize())
def phase(outdir, scenario, rate, pub_s, obs_s, run_id, kind):
    errors = []
    stop = threading.Event()
    q = queue.Queue(10000)
    threads = []
    interrupted = False
    topic = None
    subscription = None
    base = None
    log = None
    base_path = pathlib.Path(outdir)
    base_path.mkdir(parents=True, exist_ok=False)
    raw_path = base_path / "raw.jsonl"
    try:
        guard()
        if scenario not in ("A", "B", "C"):
            raise ValueError("scenario must be A, B, or C")
        if rate <= 0:
            raise ValueError("rate must be positive")
        if pub_s > obs_s:
            raise ValueError("pub_s must be <= obs_s")

        log = Logger(str(raw_path))
        base, pub, sub = clients(log)


        topic = f"projects/{PROJECT}/topics/t-{uuid.uuid4()}"
        subscription = f"projects/{PROJECT}/subscriptions/s-{uuid.uuid4()}"
        pub.create_topic(request={"name": topic}, retry=None, timeout=2)
        sub.create_subscription(
            request={"name": subscription, "topic": topic, "ack_deadline_seconds": 10},
            retry=None,
            timeout=2,
        )
        got = sub.get_subscription(request={"subscription": subscription}, retry=None, timeout=2)
        if got.ack_deadline_seconds != 10:
            raise RuntimeError
        t0 = now() + 50_000_000
        anchor_perf_ns = now()
        anchor_utc_ns = time.time_ns()
        pub_end_ns = t0 + int(pub_s * 1e9)
        cut_ns = t0 + int(obs_s * 1e9)
        meta = {
            "run_id": run_id,
            "kind": kind,
            "scenario": scenario,
            "rate": rate,
            "pub_s": pub_s,
            "obs_s": obs_s,
            "t0_ns": t0,
            "pub_end_ns": pub_end_ns,
            "cut_ns": cut_ns,
            "anchor_perf_ns": anchor_perf_ns,
            "anchor_utc_ns": anchor_utc_ns,
            "topic": topic,
            "subscription": subscription,
            "source_sha": sourcehash(),
        }
        log.log("meta", **meta)

        producer, receiver = workers(log, pub, sub, topic, subscription, scenario, rate, pub_s, t0, pub_end_ns, cut_ns, stop, errors, q)
        threads.append(threading.Thread(target=producer, name="producer", daemon=True))
        threads.append(threading.Thread(target=receiver, name="receiver", daemon=True))
        if scenario in ("A", "C"):
            threads.append(threading.Thread(target=ack_worker, args=(log, sub, subscription, scenario, cut_ns, stop, errors, q), name="ack_worker", daemon=True))

        for th in threads:
            th.start()

        while now() < cut_ns and not stop.is_set():
            time.sleep(min(0.01, max(0.0, (cut_ns - now()) / 1e9)))

    except KeyboardInterrupt:
        interrupted = True
        errors.append("interrupted")
        if log:
            log.log("error", where="phase", error="KeyboardInterrupt")
    except Exception as exc:
        errors.append(repr(exc))
        if log:
            log.log("error", where="phase", error=repr(exc))
    finally:
        stop.set()
        deadline = now() + 2_000_000_000
        for th in threads:
            if th.ident is not None:
                remaining = max(0.0, (deadline - now()) / 1e9)
                th.join(timeout=remaining)
        if base is not None and any(th.is_alive() for th in threads):
            try:
                base.close()
            except Exception as exc:
                errors.append(repr(exc))
                if log:
                    log.log("error", where="base_close", error=repr(exc))
            errors.append("critical")
        try:
            try:
                if subscription is not None and sub is not None:
                    sub.delete_subscription(request={"subscription": subscription}, retry=None, timeout=2)
            except Exception as exc:
                errors.append(repr(exc))
                if log:
                    log.log("cleanup_subscription", ok=False, error=repr(exc))
            try:
                if topic is not None and pub is not None:
                    pub.delete_topic(request={"topic": topic}, retry=None, timeout=2)
            except Exception as exc:
                errors.append(repr(exc))
                if log:
                    log.log("cleanup_topic", ok=False, error=repr(exc))
            if log:
                log.log("cleanup", ok=True)
        except Exception as exc:
            errors.append(repr(exc))
            if log:
                log.log("cleanup", ok=False, error=repr(exc))
        finally:
            try:
                if base is not None:
                    try:
                        base.close()
                    except Exception as exc:
                        errors.append(repr(exc))
                        if log:
                            log.log("error", where="base_close_final", error=repr(exc))
            finally:
                if log is not None:
                    try:
                        log.close()
                    except Exception as exc:
                        if errors is not None:
                            errors.append(repr(exc))
                if log is not None and getattr(log, "errors", None):
                    if errors is not None:
                        errors.extend(log.errors)

    return {
        "errors": errors,
        "interrupted": interrupted,
        "critical": any(th.is_alive() for th in threads),
        "raw": str(raw_path),
        "meta": {
            "run_id": run_id,
            "kind": kind,
            "scenario": scenario,
            "rate": rate,
            "pub_s": pub_s,
            "obs_s": obs_s,
            "t0_ns": t0 if "t0" in locals() else None,
            "pub_end_ns": pub_end_ns if "pub_end_ns" in locals() else None,
            "cut_ns": cut_ns if "cut_ns" in locals() else None,
            "anchor_perf_ns": anchor_perf_ns if "anchor_perf_ns" in locals() else None,
            "anchor_utc_ns": anchor_utc_ns if "anchor_utc_ns" in locals() else None,
            "topic": topic,
            "subscription": subscription,
            "source_sha": sourcehash(),
            "valid": (not errors) and (not interrupted),
        },
    }

