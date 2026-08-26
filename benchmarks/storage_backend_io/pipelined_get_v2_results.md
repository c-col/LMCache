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
# 1. find the LMCache checkout the bench venv actually uses
#    ("Editable project location" is the checkout; if there is no Editable
#    line, lmcache was installed from a wheel — clone the fork instead)
pip show lmcache | grep -Ei 'location|editable'
cd <editable-project-location>
```

```bash
# 2. make sure the checkout can see the fork, then fetch
git remote -v
# if no remote points at github.com/c-col/LMCache, add one:
#   git remote add ccol https://github.com/c-col/LMCache.git
git fetch --all
```

```bash
# 3. switch to the branch (stash first if the tree is dirty --
#    `git status --short` -- so nothing local is lost)
git checkout pipelined-get-v2
git log --oneline -3   # expect: 894c7f9e / 3b8fc7af / 501ac4d2
```

```bash
# 4. rebuild -- REQUIRED: the C++ extension changed (new ctor arg, new
#    bindings), so the old .so will not match the Python plumbing.
#    Run inside the fork venv; takes a few minutes.
uv pip install -e . --no-build-isolation
```

```bash
# 5. verify the new surface is live before benchmarking
python -c "
from lmcache.lmcache_redis import LMCacheRedisClient as C
assert '_plan_get_tiles' in dir(C) and 'error_counters' in dir(C)
assert 'get_target_tile_bytes' in (C.__init__.__doc__ or '')
print('pipelined-get-v2 extension OK')"
```

If the host checkout was never a git clone (wheel install), clone fresh
instead: `git clone -b pipelined-get-v2 https://github.com/c-col/LMCache.git
&& cd LMCache && uv pip install -e . --no-build-isolation` (in the venv).

For rapid iteration on *uncommitted* local edits, rsync from the Mac instead
of going through GitHub (run locally, not on the host):
`rsync -az --exclude .git --exclude '*.so' ~/Documents/LMCache-fork/
<user>@<g7-host>:<checkout-path>/` — then rerun step 4 on the host.

## 0. Prerequisites (record before any numbers)

```bash
# rebuild the wheel on the bench host, in the fork venv
uv pip install -e . --no-build-isolation
pytest -x tests/v1/storage_backend/test_redis_native_batch_integration.py -q
pytest -x tests/v1/distributed/test_native_connector_l2_adapter.py -q
pytest -q tests/v1/mp_observability/subscribers/metrics/
```

```bash
# on the Redis EC2 host: proxy threads MUST be 24
sudo /opt/redislabs/bin/rladmin info proxy all | grep -i thread
# if not: sudo /opt/redislabs/bin/rladmin tune proxy all threads 24
```

Record here:

- RAM DB: type/shards/proxy threads: ____
- Flex DB: type/shards/proxy threads/RAM-tier size: ____
- Flex seed/settle protocol used (flush → seed → sleep 90): ____
- Client instance / kernel / date: ____

## 1. Sweep matrix

Three configs at each sweep point, single client, on BOTH the 8-shard 100%
RAM DB and the 8-shard Flex DB (RAM-resident keyspace):

- (i) original baseline: `--get-batch-mode single`
- (ii) old default: `--get-batch-mode pipeline --get-min-keys-per-tile 8`
- (iii) new: `--get-batch-mode pipeline --get-target-tile-mb 32`

Sweep points (chunk64/128/256 ≈ 8.4/16.8/33.6 MB values):

```bash
for CFG in "--get-batch-mode single" \
           "--get-batch-mode pipeline --get-min-keys-per-tile 8" \
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

### 1a. RAM DB (8 shards)

(paste `==` headers, per-batch stage lines, aggregate lines, and CSV rows
verbatim per run)

| point | (i) single | (ii) min_keys=8 | (iii) target 32MB |
|---|---|---|---|
| 125 × 16.8 MB | | | |
| 250 × 8.4 MB | | | |
| 62 × 33.6 MB | | | |
| 500 × 4.2 MB | | | |

### 1b. Flex DB, RAM-resident (8 shards)

(same matrix)

| point | (i) single | (ii) min_keys=8 | (iii) target 32MB |
|---|---|---|---|
| 125 × 16.8 MB | | | |
| 250 × 8.4 MB | | | |
| 62 × 33.6 MB | | | |
| 500 × 4.2 MB | | | |

## 2. Flex spilled-keyspace point

```bash
# flush the Flex DB, then seed 500 x 33.6 MB and let tiering settle
python benchmarks/storage_backend_io/connector_stage_bench.py \
  --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
  --num-workers 16 --num-keys 500 --value-mb 33.6 --batches 1 --warmup 0 \
  --key-prefix flexspill --no-cleanup
sleep 90

for CFG in "--get-batch-mode pipeline --get-min-keys-per-tile 8" \
           "--get-batch-mode pipeline --get-target-tile-mb 32"; do
  python benchmarks/storage_backend_io/connector_stage_bench.py \
    --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
    --num-workers 64 --num-keys 500 --value-mb 33.6 --batches 6 --warmup 1 \
    $CFG --key-prefix flexspill --skip-set --no-cleanup
done
```

Expect ~6–8.5 GB/s blended; do not chase more (known flash-path node
ceiling). Run-to-run ±15% is dominated by RAM-tier residency — flush + seed +
settle before comparing, and never compare across reseeds.

## 3. Acceptance criteria

| # | criterion | met? |
|---|---|---|
| 1 | (iii) ≥ (ii) at every sweep point, and ≥ (i) everywhere (<5% single-point regressions acceptable if flagged) | |
| 2 | (iii) ≥ 15 GB/s at 125 × 16.8 MB on the RAM DB | |
| 3 | (iii) within noise of best of (i)/(ii) at 250 × 8.4 MB | |
| 4 | queue/dispatch/handoff each < 2 ms at inflight=1 (per-batch stage lines) | |
| 5 | Flex spilled point: (iii) ≥ (ii) | |

## 4. Conclusions

(fill after runs)

## 5. Deviations from PIPELINED_GET_SPEC.md

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
