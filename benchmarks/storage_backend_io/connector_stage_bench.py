# SPDX-License-Identifier: Apache-2.0
"""Standalone stage-timing microbenchmark for the native Redis connector.

Drives the raw pybind client (``lmcache.lmcache_redis.LMCacheRedisClient``)
directly — no LMCache Python layers, no MP server, no vLLM — and reads the
connector's per-batch stage timestamps via ``drain_batch_timings()``. This
isolates the C++ connector + wire + Redis from everything above it: if this
benchmark sustains far more throughput than the full serving stack, the
bottleneck is above the connector (staging/MP transfer/injection); if it
plateaus at the same rate, the connector is the target.

Stages per batch (all from the connector's monotonic-anchored clock, except
``handoff`` whose end is this process's ``time.time()`` at drain):

  queue    = t_first_dequeue - t_submit        (waiting for a worker thread)
  dispatch = t_first_byte    - t_first_dequeue (send + backend service; GET only)
  transfer = t_last_done     - t_first_byte    (payload streaming, all tiles)
  handoff  = t_consumed      - t_last_done     (eventfd wakeup + drain)
  total    = t_consumed      - t_submit

Example (sweep workers and value size against the benchmark Redis):

  python benchmarks/storage_backend_io/connector_stage_bench.py \\
      --host 172.31.40.215 --port 6379 \\
      --num-workers 8,16,32,64 --num-keys 125 --value-mb 16.8 \\
      --batches 8 --warmup 2
"""

# Future
from __future__ import annotations

# Standard
import argparse
import os
import select
import statistics
import sys
import time


def _make_payload(index: int, size: int) -> bytearray:
    """Deterministic binary payload embedding CRLF/NUL bytes (RESP hazard)."""
    base = (b"\r\n\x00\xff" + index.to_bytes(4, "big") + b"payload!") * (size // 16 + 1)
    return bytearray(base[:size])


class SyncTimedClient:
    """Synchronous wrapper over the native client that collects stage timings.

    Same pattern as the ``_SyncClient`` test harness in
    tests/v1/storage_backend/test_redis_native_batch_integration.py, extended
    to drain ``drain_batch_timings()`` right after ``drain_completions()``
    (the connector guarantees a timing record exists for every completion
    just drained) and stamp ``t_consumed``.
    """

    def __init__(self, args: argparse.Namespace, num_workers: int):
        # First Party
        from lmcache.lmcache_redis import LMCacheRedisClient

        self.client = LMCacheRedisClient(
            args.host,
            args.port,
            num_workers,
            args.username,
            args.password,
            args.get_min_keys_per_tile,
            args.get_batch_mode,
            "pipeline",
        )
        self.poll = select.poll()
        self.poll.register(self.client.event_fd(), select.POLLIN)
        self.pending: dict[int, tuple[bool, str, list[bool] | None]] = {}
        # future_id -> (op, num_keys, total_bytes, t_submit, t_first_dequeue,
        #               t_first_byte, t_last_done, t_consumed)
        self.timings: dict[int, tuple] = {}

    def _wait(self, future_id: int, timeout_s: float = 120.0):
        while future_id not in self.pending:
            events = self.poll.poll(timeout_s * 1000)
            if not events:
                raise TimeoutError(f"future {future_id} never completed")
            completions = self.client.drain_completions()
            t_consumed = time.time()
            for fid, ok, err, bools in completions:
                self.pending[fid] = (ok, err, bools)
            for fid, op, nk, nb, t0, t1, t2, t3 in self.client.drain_batch_timings():
                self.timings[fid] = (op, nk, nb, t0, t1, t2, t3, t_consumed)
        ok, err, _ = self.pending.pop(future_id)
        if not ok:
            raise RuntimeError(f"batch failed: {err}")
        return self.timings.pop(future_id, None)

    def batch_set(self, keys: list[str], payloads: list[bytearray]):
        views = [memoryview(p) for p in payloads]
        return self._wait(self.client.submit_batch_set(keys, views))

    def batch_get(self, keys: list[str], bufs: list[bytearray]):
        views = [memoryview(b) for b in bufs]
        return self._wait(self.client.submit_batch_get(keys, views))

    def batch_delete(self, keys: list[str]):
        return self._wait(self.client.submit_batch_delete(keys))

    def close(self):
        self.client.close()


def _stages_ms(timing: tuple) -> dict[str, float]:
    op, nk, nb, t_submit, t_dequeue, t_first_byte, t_last_done, t_consumed = timing
    transfer_start = t_first_byte if t_first_byte > 0.0 else t_dequeue
    return {
        "queue": (t_dequeue - t_submit) * 1e3,
        "dispatch": ((t_first_byte - t_dequeue) * 1e3 if t_first_byte > 0.0 else 0.0),
        "transfer": (t_last_done - transfer_start) * 1e3,
        "handoff": max(0.0, t_consumed - t_last_done) * 1e3,
        "total": max(0.0, t_consumed - t_submit) * 1e3,
    }


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(p / 100.0 * len(s)))]


def run_sweep_point(
    args: argparse.Namespace, num_workers: int, num_keys: int, value_bytes: int
) -> None:
    keys = [
        f"stagebench{_KEY_NONCE}_{value_bytes}_{i}" for i in range(num_keys)
    ]
    payloads = [_make_payload(i, value_bytes) for i in range(num_keys)]
    total_gb = num_keys * value_bytes / 1e9

    client = SyncTimedClient(args, num_workers)
    try:
        client.batch_set(keys, payloads)

        op = args.op
        bufs = [bytearray(value_bytes) for _ in range(num_keys)]

        def run_batch():
            if op == "get":
                return client.batch_get(keys, bufs)
            return client.batch_set(keys, payloads)

        for _ in range(args.warmup):
            run_batch()

        per_stage: dict[str, list[float]] = {
            k: [] for k in ("queue", "dispatch", "transfer", "handoff", "total")
        }
        gbps: list[float] = []
        print(
            f"\n== op={op} workers={num_workers} keys={num_keys} "
            f"value={value_bytes / 1e6:.2f}MB batch={total_gb:.3f}GB "
            f"mode={args.get_batch_mode} min_keys_per_tile="
            f"{args.get_min_keys_per_tile}"
        )
        for b in range(args.batches):
            timing = run_batch()
            if timing is None:
                print(
                    "  (no timing record — is the connector built with "
                    "drain_batch_timings?)"
                )
                return
            st = _stages_ms(timing)
            rate = total_gb / (st["total"] / 1e3) if st["total"] > 0 else 0.0
            gbps.append(rate)
            for k, v in st.items():
                per_stage[k].append(v)
            print(
                f"  batch {b}: queue {st['queue']:8.2f}ms | "
                f"dispatch {st['dispatch']:8.2f}ms | "
                f"transfer {st['transfer']:8.2f}ms | "
                f"handoff {st['handoff']:7.2f}ms | "
                f"total {st['total']:8.2f}ms | {rate:6.2f} GB/s"
            )

        print("  stage       mean        p50        p99   (ms)")
        for k in ("queue", "dispatch", "transfer", "handoff", "total"):
            vals = per_stage[k]
            print(
                f"  {k:<9s} {statistics.mean(vals):9.2f} "
                f"{_pct(vals, 50):10.2f} {_pct(vals, 99):10.2f}"
            )
        # machine-readable summary (grep '^CSV')
        print(
            "CSV,"
            + ",".join(
                str(x)
                for x in (
                    op,
                    args.get_batch_mode,
                    num_workers,
                    num_keys,
                    value_bytes,
                    round(statistics.mean(per_stage["queue"]), 3),
                    round(statistics.mean(per_stage["dispatch"]), 3),
                    round(statistics.mean(per_stage["transfer"]), 3),
                    round(statistics.mean(per_stage["handoff"]), 3),
                    round(statistics.mean(per_stage["total"]), 3),
                    round(statistics.mean(gbps), 3),
                    round(_pct(gbps, 50), 3),
                )
            )
        )
    finally:
        if args.cleanup:
            try:
                client.batch_delete(keys)
            except Exception as exc:  # noqa: BLE001
                print(f"  cleanup failed: {exc}", file=sys.stderr)
        client.close()


# distinguishes concurrent/repeated runs so leftover keys never collide
_KEY_NONCE = os.getpid()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument(
        "--username", default=os.environ.get("LMCACHE_RESP_USERNAME", "")
    )
    parser.add_argument(
        "--password", default=os.environ.get("LMCACHE_RESP_PASSWORD", "")
    )
    parser.add_argument(
        "--num-workers",
        default="32",
        help="comma-separated sweep list, e.g. 8,16,32,64",
    )
    parser.add_argument(
        "--num-keys",
        default="125",
        help="comma-separated sweep list of keys per batch",
    )
    parser.add_argument(
        "--value-mb",
        default="16.8",
        help="comma-separated sweep list of value sizes in MB (decimal)",
    )
    parser.add_argument("--batches", type=int, default=8)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--op", choices=["get", "set"], default="get")
    parser.add_argument(
        "--get-batch-mode", choices=["pipeline", "mget"], default="pipeline"
    )
    parser.add_argument("--get-min-keys-per-tile", type=int, default=8)
    parser.add_argument(
        "--no-cleanup",
        dest="cleanup",
        action="store_false",
        help="leave benchmark keys in Redis after each sweep point",
    )

    args = parser.parse_args()
    print(
        "CSV,op,get_batch_mode,num_workers,num_keys,value_bytes,"
        "queue_ms,dispatch_ms,transfer_ms,handoff_ms,total_ms,"
        "gbps_mean,gbps_p50"
    )
    for workers in [int(x) for x in args.num_workers.split(",")]:
        for nkeys in [int(x) for x in args.num_keys.split(",")]:
            for vmb in [float(x) for x in args.value_mb.split(",")]:
                run_sweep_point(args, workers, nkeys, int(vmb * 1e6))


if __name__ == "__main__":
    main()
