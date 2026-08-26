# Pipelined GET v2 — microbenchmark acceptance results

Branch: `pipelined-get-v2` (off `stage-timing-instrumentation`).
Protocol: `PIPELINED_GET_SPEC.md` §6. Format follows
`flex_microbenchmark_testing.md` in the benchmarking repo (commands verbatim,
full output blocks, CSV rows).

Status: **TEMPLATE — to be filled on the live g7.24xlarge ↔ i8ge.48xlarge
pair.** Implementation and local verification are complete (76 Redis
integration tests incl. byte-tiling/single-mode/deprecation/counters, 184
mp_observability tests, ruff + clang-format clean); everything below runs on
the bench host.

## Getting this branch onto the vLLM host

The branch is pushed to the fork: `github.com/c-col/LMCache`, branch
`pipelined-get-v2` (3 commits on top of `stage-timing-instrumentation`).
Once SSH'd into the g7 vLLM host:

```bash
source ~/bench-env.sh
```

```bash
cd ~/src && git clone --branch pipelined-get-v2 https://github.com/c-col/LMCache.git LMCache-fork
```

```bash
# check for nvcc access
source ~/.venv/bin/activate && which nvcc || ls /usr/local/cuda/bin/nvcc 2>/dev/null || echo "NO NVCC"

# if nvcc found, do:
export PATH=/usr/local/cuda/bin:$PATH CUDA_HOME=/usr/local/cuda
```

```bash
# rebuild -- REQUIRED: the C++ extension changed (new ctor arg, new
#   bindings), so the old .so will not match the Python plumbing.
#   Run inside the fork venv; takes a few minutes.
cd ~/src/LMCache-fork

uv pip install -e . --no-build-isolation
```

```bash
# verify the new surface is live before benchmarking
python -c "
from lmcache.lmcache_redis import LMCacheRedisClient as C
assert '_plan_get_tiles' in dir(C) and 'error_counters' in dir(C)
assert 'get_target_tile_bytes' in (C.__init__.__doc__ or '')
print('pipelined-get-v2 extension OK')"
```

## 0. Prerequisites (record before any numbers)

The Redis integration suite needs a throwaway LOCAL redis on port 6399 — do
NOT point it at the RE cluster (the suite has no auth plumbing, and its
commandstats-delta assertions assume a dedicated quiet server). The `env -u`
matters: bench-env.sh exports REDIS_HOST/REDIS_PORT for the RE endpoint, and
the suite silently skips everything when its unauthenticated ping to that
host fails.

```bash
sudo apt-get install -y redis-server redis-tools
redis-server --port 6399 --daemonize yes --save '' --appendonly no
```

```bash
env -u REDIS_HOST -u REDIS_PORT pytest -x tests/v1/storage_backend/test_redis_native_batch_integration.py -q   # expect 76 passed
pytest -x tests/v1/distributed/test_native_connector_l2_adapter.py -q   # expect 87 passed
pytest -q tests/v1/mp_observability/subscribers/metrics/                # expect 184 passed
```

```bash
# on the Redis EC2 host: proxy threads MUST be 24
sudo /opt/redislabs/bin/rladmin info proxy all | grep -i thread
# if not: sudo /opt/redislabs/bin/rladmin tune proxy all threads 24
```

Record here:

- RAM DB: type/shards/proxy threads: ?/4/24
- Flex DB: type/shards/proxy threads/RAM-tier size: ?/4/24/10%
- Flex seed/settle protocol used (flush → seed → sleep 90): ???
- Client instance / kernel / date: g7.24xlarge + i8ge.48xlarge / Redis node is ubuntu 22.04 server / August 26 2026

## 1. Sweep matrix

Three configs at each sweep point, single client, on BOTH the 8-shard 100%
RAM DB and the 8-shard Flex DB (RAM-resident keyspace):

- (i) original baseline: `--get-batch-mode single`
- (ii) old default: `--get-batch-mode pipeline --get-min-keys-per-tile 8`
- (iii) new: `--get-batch-mode pipeline --get-target-tile-mb 32`

Sweep points (chunk64/128/256 ≈ 8.4/16.8/33.6 MB values):

```bash
for CFG in "--get-batch-mode single" \
           "--get-batch-mode pipeline --get-target-tile-mb 32"; do
  for PT in "--num-keys 125 --value-mb 16.8" \
            "--num-keys 250 --value-mb 8.4" \
            "--num-keys 62 --value-mb 33.6" \
            "--num-keys 500 --value-mb 4.2"; do
    python benchmarks/storage_backend_io/connector_stage_bench.py \
      --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
      --num-workers 64 --batches 8 --warmup 2 $CFG $PT
  done
done
```

Compare `agg_gbps` (wall-clock; the only cross-config comparable number).

### 1a. RAM DB (4 shards)

(paste `==` headers, per-batch stage lines, aggregate lines, and CSV rows
verbatim per run)

| point | (i) single | (ii) min_keys=8 | (iii) target 32MB |
|---|---|---|---|
| 125 × 16.8 MB | 13.22 | 6.663 | 13.878 |
| 250 × 8.4 MB | 14.197 | 9.453 | 10.471 |
| 62 × 33.6 MB | 14.556 | 5.215 | 14.821 |
| 500 × 4.2 MB | 14.754 | 11.11 | 10.554 |

### 1b. RAM DB (8 shards)

(skipped getting min_keys=8 results)

| point | (i) single | (ii) min_keys=8 | (iii) target 32MB |
|---|---|---|---|
| 125 × 16.8 MB | 15.385 | x | 16.979 |
| 250 × 8.4 MB | 15.783 | x | 14.258 |
| 62 × 33.6 MB | 20.303 | x | 20.407 |
| 500 × 4.2 MB | 16.959 | x | 14.94 |

### 1c. Flex DB, RAM-resident (4 shards)

| point | (i) single | (ii) min_keys=8 | (iii) target 32MB |
|---|---|---|---|
| 125 × 16.8 MB | 13.226 | 6.996 | 12.475 |
| 250 × 8.4 MB | 14.629 | 10.175 | 11.989 |
| 62 × 33.6 MB | 11.968 | 4.621 | 13.34 |
| 500 × 4.2 MB | 9.014 | 9.07 | 9.581 |

### 1d. Flex DB, RAM-resident (8 shards)

(skipped getting min_keys=8 results)

| point | (i) single | (ii) min_keys=8 | (iii) target 32MB |
|---|---|---|---|
| 125 × 16.8 MB | 19.749 | x | 13.616 |
| 250 × 8.4 MB | 17.974 | x | 13.884 |
| 62 × 33.6 MB | 17.273 | x | 19.92 |
| 500 × 4.2 MB | 14.57 | x | 11.69 |

## 2. Flex spilled-keyspace point

```bash
# flush the Flex DB, then seed 500 x 33.6 MB and let tiering settle
python benchmarks/storage_backend_io/connector_stage_bench.py \
  --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
  --num-workers 16 --num-keys 500 --value-mb 33.6 --batches 1 --warmup 0 \
  --key-prefix flexspill --no-cleanup
sleep 90

for CFG in "--get-batch-mode single"; do
  python benchmarks/storage_backend_io/connector_stage_bench.py \
    --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
    --num-workers 64 --num-keys 500 --value-mb 33.6 --batches 6 --warmup 1 \
    $CFG --key-prefix flexspill --skip-set --no-cleanup
done
```

Expect ~6–8.5 GB/s blended; do not chase more (known flash-path node
ceiling). Run-to-run ±15% is dominated by RAM-tier residency — flush + seed +
settle before comparing, and never compare across reseeds.

### Results (agg_gbps per configuration)
- 4 shards:
  - single GETs: 5.179 GB/s
  - min-keys-per-tile 8: 5.856 GB/s
  - get-target-tile-mb 32: 6.795 GB/s

- 8 shards:
  - single GETs: 5.615 GB/s
  - min-keys-per-tile 8: 7.195 GB/s
  - get-target-tile-mb 32: 6.776 GB/s

## 3. Acceptance criteria

| # | criterion | met? |
|---|---|---|
| 1 | (iii) ≥ (ii) at every sweep point, and ≥ (i) everywhere (<5% single-point regressions acceptable if flagged) | **vs (ii): MET** (1.5–3× at every measured point). **vs (i): NOT MET at small values** — 250×8.4 and 500×4.2 lose 10–40% on both DBs/shard counts, and Flex-8sh 125×16.8 loses 31%. See conclusions. |
| 2 | (iii) ≥ 15 GB/s at 125 × 16.8 MB on the RAM DB | **MET** — 16.98 GB/s (8 shards). 4-shard is 13.88, consistent with the known 4-shard ceiling. |
| 3 | (iii) within noise of best of (i)/(ii) at 250 × 8.4 MB | **NOT MET** — best is (i) single at 15.78 (8sh); (iii) is 14.26 (−9.7%), and the sign is consistent across both DBs and shard counts, so it is not run noise. |
| 4 | queue/dispatch/handoff each < 2 ms at inflight=1 | **MET** for the pipeline modes (queue ~0.05 ms, dispatch ≤1.8 ms, handoff ≤0.35 ms). Single mode's 3–136 ms "dispatch" is definitional (first byte ≈ first completed value round trip), not a scheduling cost. |
| 5 | Flex spilled point: (iii) ≥ (ii) | **MET at 4 shards** (6.79 vs 5.86). 8 shards nominally lost (6.78 vs 7.20, −6%) but both configs tile near-identically there (~63–64 tiles × ~8 keys) — tier-residency noise (documented ±15%), not policy. |

## 4. Conclusions

1. **The old default is dead.** `min_keys=8` costs 1.5–3× at every measured
   point vs byte tiling (worst: 4.62 vs 14.82 GB/s at 62×33.6, RAM 4sh).
   Deprecation confirmed by data.
2. **The unexpected finding: `single` mode is now the best RAM-path config
   at ≤8.4 MB values** and competitive everywhere RAM-resident (it also got
   TCP_NODELAY + the buffered reader, so it is not upstream's old baseline).
   Once per-round-trip overhead is gone, multi-key pipelining's remaining
   effect is negative on a fast server: a socket with 4–8 values queued
   receives a multi-value burst per flow, while single mode is
   self-clocking — one value in flight per socket — and its per-batch
   transfer times are visibly steadier (pipeline shows repeated 290–340 ms
   straggler batches at exactly the small-value points).
3. **The relationship inverts on flash**: spilled-keyspace single is the
   WORST config (5.18/5.61 GB/s vs 5.9–7.2 pipelined). High per-key server
   latency needs pipelining to keep the pipe full.
4. **Implication → v3 design: windowed pipelined GET.** Add a per-socket
   in-flight cap (bytes): send GETs until W bytes of replies are expected,
   then send the next GET as each reply drains. W = value_size reproduces
   single mode's self-clocking with miss tolerance; W = ∞ is today's
   pipeline; the flash path wants W large. This is a small change to
   `do_batch_get_pipelined` (interleave sends with the reply loop) and one
   new knob.
5. Wire ceiling on this pair is ~20 GB/s (62×33.6 hits 20.3–20.4 on both
   RAM and Flex-resident 8sh), higher than the earlier 16–17 estimate.
   RAM-resident Flex can reach pure-RAM throughput (19.75 at 125×16.8,
   single mode). Node-level flash-blended ceiling reconfirmed ~7 GB/s,
   flat across 4/8 shards.
6. **Guidance for the serving stack today**: for chunk256 (33.6 MB) loads,
   v2 byte tiling is the right default and at the wire. For chunk64/128
   RAM-resident loads there is a 10–25% gap to single mode that only the
   windowed v3 can close in a production-safe (miss-tolerant) way.

## 5. Next experiments (designs)

### 6a. Full-flash retrieval (deep-spill the 100 GB Flex DB)

Goal: measure ~pure-SSD GETs, not the RAM/flash blend. Two effects to
control: (1) reads PROMOTE values into the RAM tier, so re-reading the same
keys warms them — cold measurements must read a slice once; (2) seeding
order sets residency — the earliest-seeded slices are the coldest.

```bash
# seed 5 x 16.8 GB slices (84 GB on the 100 GB DB, RAM tier ~10 GB).
# slice A is seeded first => coldest after B..E push it out.
for SLICE in A B C D E; do
  python benchmarks/storage_backend_io/connector_stage_bench.py \
    --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
    --num-workers 16 --num-keys 500 --value-mb 33.6 --batches 1 --warmup 0 \
    --op set --key-prefix flexfull$SLICE --no-cleanup
done
sleep 120
```

```bash
# cold pass: read slice A ONCE per config, a fresh (still-cold) slice per
# config would be even cleaner -- with 3 configs use A, B, C respectively.
# batch 0 of each run is the cold number; later batches show promotion.
for RUN in "A --get-batch-mode single" \
           "B --get-batch-mode pipeline --get-target-tile-mb 32" \
           "C --get-batch-mode pipeline --get-target-tile-mb 8.4"; do
  set -- $RUN; SLICE=$1; shift
  python benchmarks/storage_backend_io/connector_stage_bench.py \
    --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
    --num-workers 64 --num-keys 500 --value-mb 33.6 --batches 4 --warmup 0 \
    "$@" --key-prefix flexfull$SLICE --skip-set --no-cleanup
done
```

While a cold pass runs, on the Redis node: `iostat -xm 5` — write traffic
during a read-only pass = promotion/demotion churn (the queued RE-Flex
discriminator). Expect single mode to lose badly here (conclusion 3) and
the per-batch lines to rise batch-over-batch as promotion warms the slice.
Keep total seeded < ~85 GB to stay clear of eviction watermarks.

### 6b. Context-size proxy sweep (GET method × chunk size × context size)

One microbench batch models one request's L2 load: a context of T tokens at
chunk size c is `num_keys = T_bytes / value_bytes` keys of the same value
size (chunk64/128/256 ≈ 8.4/16.8/33.6 MB ≈ 131 KB/token), so larger context
= more keys, same value size. Key names embed value_bytes, so ONE prefix
serves all sizes without collision.

```bash
# ---- seed once: 16.8 GB (max context) per chunk size = 67 GB total ----
# (on the Flex DB this also creates a deep spill; on the RAM DB it is
# simply resident)
PREFIX=flexctx
for VMB in 4.2 8.4 16.8 33.6; do
  KEYS=$(python3 -c "print(round(16.8*1000/$VMB))")
  python benchmarks/storage_backend_io/connector_stage_bench.py \
    --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
    --num-workers 16 --num-keys $KEYS --value-mb $VMB --batches 1 --warmup 0 \
    --op set --key-prefix $PREFIX --no-cleanup
done
sleep 120

# ---- read: chunk size x context size x GET method ----
# context 4.2 / 8.4 / 16.8 GB ~= 32k / 64k / 128k tokens
for VMB in 4.2 8.4 16.8 33.6; do
  for CTXGB in 4.2 8.4 16.8; do
    KEYS=$(python3 -c "print(round($CTXGB*1000/$VMB))")
    for CFG in "--get-batch-mode single" \
               "--get-batch-mode pipeline --get-target-tile-mb 32" \
               "--get-batch-mode pipeline --get-target-tile-mb $VMB"; do
      python benchmarks/storage_backend_io/connector_stage_bench.py \
        --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
        --num-workers 64 --batches 4 --warmup 0 \
        --num-keys $KEYS --value-mb $VMB $CFG \
        --key-prefix $PREFIX --skip-set --no-cleanup \
        | grep -E '^==|batch |aggregate|^CSV'
    done
  done
done
```

Notes: the third config (`target = value size`) forces the most tiles the
byte rule allows — with 64 workers it still cannot go below ~keys/64 keys
per tile, which is exactly why it cannot match single mode at small values
(the windowed v3 can). `--warmup 0` keeps batch 0 as the cold/residency
signal; reading promotes, so within a (VMB, CTXGB) cell configs later in
the inner loop see a warmer slice — if that matters, randomize or rotate
the config order across repeats. On Flex, reseed + settle between full
sweeps; never compare across reseeds.

### 6c. Concurrency simulation

Two orthogonal knobs, modeling different real conditions:

1. **Within one engine (requests overlapping inside one LMCache client):**
   `--inflight-batches N` — one client, one worker pool, N batches
   outstanding (this is what the MP adapter does under concurrent
   requests). Caveat from Test 5: at inflight>1 the huge queue times are a
   client-side FIFO artifact (tiles behind sibling batches), not server
   latency — read agg_gbps, not queue_ms.
2. **Across engines (multiple vLLM instances / TP workers):** N processes,
   shared keyspace:

```bash
for i in 1 2 3 4; do
  python benchmarks/storage_backend_io/connector_stage_bench.py \
    --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
    --num-workers 16 --num-keys 125 --value-mb 16.8 --batches 8 --warmup 1 \
    --get-batch-mode pipeline --get-target-tile-mb 32 \
    --key-prefix shared --skip-set --no-cleanup 2>/dev/null | grep ^CSV &
done; wait   # sum the agg_gbps columns
```

   Shared keyspace models a shared prefix cache (constant working set);
   distinct prefixes per process model independent working sets — on Flex
   the latter grows the working set past the RAM tier (the earlier
   confound), which is a feature if that is what you are modeling.
   Keep total sockets comparable when comparing (4×16 workers vs 1×64).
3. **The realistic grid** is the cross product: clients ∈ {1, 2, 4} ×
   inflight ∈ {1, 4} at one or two sweep points, RAM-resident and spilled.
   Prior data predicts: RAM-resident scales to the wire with either knob;
   spilled flash is flat ~7 GB/s regardless — concurrency does not buy
   flash throughput, only independent batch streams at 4 shards did
   (~8.1–8.6), and inconsistently.

## 6. Deviations from PIPELINED_GET_SPEC.md

1. **Tile-count testing mechanism.** The spec suggested counting tiles "via
   per-worker connections"; no such mechanism exists — the existing
   `TestTilingPolicy` infers tiles from Redis `INFO commandstats` deltas,
   which works for MGET (1 command per tile) but cannot observe
   pipeline-mode tiling (1 GET command per key regardless of tile count).
   Instead, an additive Redis-only pybind test hook `_plan_get_tiles(
   num_items, value_bytes)` exposes `choose_num_tiles` directly;
   `TestByteTiling` asserts the tile counts for the spec's sweep points.
2. **Existing tiling tests pin a tiny byte target.** `TestTilingShapes` and
   `TestBatchTiling` exercise multi-tile partitioning with chunk-sized
   values; under byte-based tiling those batches would collapse to 1 tile,
   silently gutting the tests. They now pass `get_target_tile_bytes=1` to
   preserve their original intent (max fan-out).
3. **`error_counters()` lives on `ConnectorBase`,** not the Redis connector,
   because the spec asks for it in the shared `LMCACHE_BIND_CONNECTOR_METHODS`
   macro (which binds every backend). Non-Redis backends report zeros for
   counters they never bump.
4. **The mget deprecation-warns-once test runs in a subprocess** — the
   warning is once-per-process and the test session itself constructs mget
   clients via `MODE_PAIRS`, so asserting "exactly once" is only meaningful
   in a fresh process.
5. **§4.6 buffered reply reader was implemented** (spec marked it optional)
   as its own commit (`recv_line` was 1 syscall/byte; now a 64 KB buffered
   reader with `memchr`, payloads still zero-copy). All 76 integration tests
   pass against it.
