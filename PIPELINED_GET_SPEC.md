# Spec: Optimized pipelined GET for the LMCache fork's native Redis connector

Audience: an implementation agent working in the LMCache fork
(`~/Documents/LMCache-fork` locally; `github.com/c-col/LMCache`, branch off
`stage-timing-instrumentation`, which contains the stage-timing
instrumentation and microbenchmark this spec depends on). A companion
benchmarking checkout lives at
`~/Documents/GitHub/ai-research-semantic-cache/experiments/lmcache-vllm-benchmarking`
(useful only for background docs; no changes there).

## 1. Goals

1. **Make pipelined GET the sole batched-GET strategy**, tuned by measured
   data: byte-based tile sizing, TCP_NODELAY, rate-limited error logging.
2. **Deprecate MGET**: the code stays, but MGET must never be a default, a
   fallback, or a recommendation. Selecting it explicitly still works and
   warns once.
3. **Add a `"single"` GET mode** that reproduces upstream LMCache's original
   per-key round-trip behavior, so microbenchmarks can A/B (i) original GET
   vs (ii) optimized pipelined GET inside one binary.
4. **Prove the result with the stage-timing microbenchmark**
   (`benchmarks/storage_backend_io/connector_stage_bench.py`) under the
   protocol in §6, on the live g7.24xlarge ↔ i8ge.48xlarge pair.

## 2. Current state of the code (verified 2026-08-26)

All C++ lives in `csrc/storage_backends/` (header-only base + Redis backend):

- `connector_types.h` — `Op`, `BatchState` (now carries stage-timing fields:
  `t_submit`, atomic `t_first_dequeue`/`t_first_byte`, `t_last_done`,
  `num_keys`, `total_bytes`), `Request`, `Completion`, `BatchTiming`.
- `connector_base.h` — templated `ConnectorBase<ConnectionType>`: submit_*
  fan-out via `choose_num_tiles()` (virtual) + balanced `tile_bounds()`;
  worker threads in per-op lanes or a shared pool; `handle_tile_completion`
  emits one `Completion` + one `BatchTiming` per batch (co-enqueued under
  `comp_mu_`); `drain_completions()` / `drain_batch_timings()` are the
  Python-facing drains. `note_first_byte(const Request&)` is the protected
  first-byte hook (base `do_batch_get` per-key loop calls it after key 0).
- `csrc/storage_backends/redis/connector.{h,cpp}` — `WorkerConn` (blocking
  socket, RESP2, scatter-gather `send_multipart`, `recv_exactly`,
  byte-at-a-time `recv_line`) and `RedisConnector` with:
  - ctor `(host, port, num_workers, username, password,
    get_min_keys_per_tile=8, get_batch_mode="pipeline",
    exists_batch_mode="pipeline")`
  - `do_batch_get` → `do_batch_get_pipelined` (default) or
    `do_batch_get_mget`; both use `consume_bulk_value` for per-key miss /
    size-mismatch / error tolerance (drains payloads to stay in protocol
    sync) and call `note_first_byte(req)` on the first reply.
  - `choose_num_tiles(Op, num_items)`: EXISTS → 1 tile; GET →
    `min(max_tiles, ceil(num_items / get_min_keys_per_tile_))`; SET/DELETE →
    default fan-out (`min(worker_count_for_op, num_items)`).
- `csrc/storage_backends/redis/pybind.cpp` — module `lmcache_redis`, class
  `LMCacheRedisClient`, positional `py::init<...>` matching the ctor, then
  `LMCACHE_BIND_CONNECTOR_METHODS` (in `connector_pybind_utils.h`; binds
  `event_fd`, `submit_batch_*`, `drain_completions` (4-tuple — DO NOT
  change), `drain_batch_timings` (8-tuple), `close`).

Python config plumbing (two parallel paths for the same ctor):

- MP path (what benchmarks use):
  `lmcache/v1/distributed/l2_adapters/resp_l2_adapter.py` —
  `RESPL2AdapterConfig` fields `get_min_keys_per_tile` (default 8),
  `get_batch_mode` ("pipeline"), `exists_batch_mode`; `from_dict`, `help()`,
  and `_create_resp_l2_adapter` (positional ctor call) must all stay in sync.
- Non-MP path: `lmcache/v1/storage_backend/connector/redis_adapter.py` —
  `extra_config` keys `resp_get_min_keys_per_tile`, `resp_get_batch_mode`,
  `resp_exists_batch_mode` (note the rename-guard pattern there for
  `resp_mget_min_keys_per_tile` — follow it for any renamed key).
- ⚠️ `lmcache/v1/storage_backend/resp_client.py` is a dead-code duplicate of
  `native_clients/resp_client.py`. Do not touch it.

Conventions: `csrc/storage_backends/README.md` documents tiling semantics and
config-key naming; run `clang-format -i` on touched C++ and `ruff check` on
touched Python (both are in CI); positional-ctor compatibility means **new
ctor args are appended with defaults, never inserted**.

## 3. Measured context that motivates the design (do not re-derive)

From `flex_microbenchmark_testing.md` in the benchmarking repo (all with
Redis Enterprise proxy `threads: 24` — see §6 prerequisites):

- The network path sustains ~16–17 GB/s; per-TCP-flow cap is ~1.2 GB/s
  (cluster placement group, no ENA Express), so aggregate needs ≥13
  concurrently streaming sockets.
- Tile count == concurrently streaming sockets. Measured on the 8-shard RAM
  DB at 64 workers, 125 × 16.8 MB keys: 16 tiles → 9.0 GB/s, 32 → 11.6,
  64 → 16.9 (wire). The current default `get_min_keys_per_tile=8` caps that
  batch at 16 tiles — it is the wrong default for large values.
- But key-count=1 is not universally right either: at 250 × 8.4 MB,
  min_keys=8 beat min_keys=1 (10.7 vs 8.6 GB/s on Flex RAM-resident) — for
  smaller values, per-round-trip overhead matters and pipelining several
  keys per socket wins.
- Both regimes are captured by **byte-based tiling**: aim for a fixed payload
  per tile (~32 MB ≈ per-flow rate × ~25 ms), which yields ~1–2 keys/tile at
  16–33 MB values and many keys/tile at ≤4 MB values.
- MGET measured no better than pipeline anywhere, monopolizes the shard event
  loop, breaks on cross-slot batches in OSS cluster mode, and serializes at
  the RE proxy. Hence deprecation.
- Per-key error stderr spam is real: one contaminated run produced 2,500
  `[LMCache GET] ... failed` lines from worker threads.

## 4. Required changes

### 4.1 `"single"` GET mode (benchmark baseline)

- Accept `get_batch_mode = "single"` in `parse_get_batch_mode`
  (redis/connector.cpp) with a `GetBatchMode::SINGLE` enum value.
- `do_batch_get` for SINGLE delegates to the **base class** loop
  (`ConnectorBase::do_batch_get(conn, req)`), which issues one blocking GET
  round trip per key via `do_single_get` — upstream LMCache's original
  behavior, including its limitation that a miss desyncs the connection
  (document this in the mode's comment/help: single mode is a benchmarking
  baseline for fully-hit keyspaces, not a production mode).
- Tiling for SINGLE uses the default fan-out (like SET/DELETE): one tile per
  worker, since every key costs a round trip and there is nothing to
  pipeline. (This reproduces upstream's per-worker round-trip pattern.)
- Plumb `"single"` through both config paths (§2) and the microbench's
  `--get-batch-mode` choices.

### 4.2 Byte-based tiling for pipelined GET (the core change)

- New ctor arg appended after `exists_batch_mode`:
  `size_t get_target_tile_bytes = 32 * 1000 * 1000` (32 MB decimal).
- `choose_num_tiles` for `BATCH_TILE_GET` in pipeline mode becomes:
  `tiles_by_bytes = ceil(num_items * batch_chunk_num_bytes /
  get_target_tile_bytes_)`, then `clamp(tiles_by_bytes, 1, max_tiles)` where
  `max_tiles = min(worker_count_for_op(GET), num_items)`.
  - ⚠️ `choose_num_tiles` currently receives only `(Op, num_items)` — it does
    NOT see `batch_chunk_num_bytes`. Extend the virtual's signature to
    `choose_num_tiles(Op op, size_t num_items, size_t batch_chunk_num_bytes)`
    across `connector_base.h` (declaration, `prepare_batch_operation` caller
    — note EXISTS/DELETE pass 0), the Redis override, and the Mooncake
    override (`mooncake/connector.cpp`, returns 1 regardless — just update
    the signature). `prepare_batch_operation(num_items, op)` must gain the
    bytes parameter from `submit_batch_get/set` (0 for exists/delete).
  - Keep `get_min_keys_per_tile` accepted for backward compatibility but make
    it a secondary constraint: after computing `tiles_by_bytes`, do not
    exceed `ceil(num_items / get_min_keys_per_tile)` **only if the user
    explicitly set min_keys > 1** — simplest faithful rule: byte target
    governs; `get_min_keys_per_tile` default changes to 1 and its help text
    marks it deprecated in favor of the byte target.
  - MGET mode may keep the old key-count logic (it is deprecated; don't
    invest).
- Expose the new knob everywhere the existing ones are exposed:
  pybind `py::init` (append), `RESPL2AdapterConfig` (`get_target_tile_bytes`,
  from_dict + help), `redis_adapter.py` (`resp_get_target_tile_bytes`),
  microbench flag `--get-target-tile-mb` (float, default 32).

### 4.3 MGET deprecation

- `parse_get_batch_mode("mget")` continues to work but emits a one-time
  `fprintf(stderr, "[LMCache] get_batch_mode=mget is deprecated ...")`.
- Confirm (and add a comment + test asserting) that no code path falls back
  to MGET: pipelined GET failures propagate as tile failures, never
  re-dispatch. Update `csrc/storage_backends/README.md` and both config
  `help()` texts to say pipeline is the only supported mode and mget is
  deprecated.

### 4.4 TCP_NODELAY

- In `WorkerConn::connect` (redis/connector.cpp), after a successful
  `::connect`: `setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, ...)`; include
  `<netinet/tcp.h>`. Failure to set is non-fatal (log once).

### 4.5 Rate-limited per-key error logging + counters

- Replace the per-key `fprintf` calls in `consume_bulk_value` and
  `do_batch_get_pipelined` (and the base `do_batch_get`/`do_batch_delete`
  loops in `connector_base.h`) with a small helper on the connector:
  atomic counters `{get_not_found, get_size_mismatch, get_error_reply,
  delete_failed}`; print the first 5 occurrences per counter, then every
  1000th, including the running count.
- Expose the counters to Python as an **additive** method
  `error_counters() -> dict[str, int]` bound in
  `LMCACHE_BIND_CONNECTOR_METHODS` (same additive pattern as
  `drain_batch_timings`; do NOT touch the `drain_completions` 4-tuple).

### 4.6 Optional (do only if the above lands cleanly): buffered reply reader

Replace `recv_line`'s byte-per-syscall loop with a per-connection 64 KB
buffered reader (`memchr` for CRLF; `recv_exactly`/payload reads must consume
buffered bytes first). This is polish — skip if it risks the schedule; it
must not change any wire behavior.

## 5. Tests (fork repo; all must pass)

Existing suites to keep green (these encode the current contracts):

- `tests/v1/storage_backend/test_redis_native_batch_integration.py` — real
  Redis (default `localhost:6399`; start one via
  `redis-server --port 6399 --daemonize yes --save '' --appendonly no`) +
  built `lmcache_redis`. `MODE_PAIRS` at the top parametrizes
  (get_batch_mode, exists_batch_mode) — extend with `("single", "pipeline")`
  where behavior allows (note: single mode desyncs on miss, so miss-tolerance
  tests must NOT run under single mode; scope the parametrization
  accordingly rather than weakening assertions).
- `TestBatchTimings` (same file) — stage-timing records; single mode gets
  first-byte from the base-loop hook, so timing tests should pass unchanged.
- `tests/v1/distributed/test_native_connector_l2_adapter.py`,
  `tests/v1/mp_observability/subscribers/metrics/` — must pass untouched.

New tests to add:

1. Byte-based tiling shapes (extend `TestTilingShapes`/`TestTilingPolicy`
   pattern, which counts tiles via per-worker connections): e.g. 125 keys ×
   16.8 MB with target 32 MB → ~66 tiles capped by workers; 250 × 4.2 MB
   with target 32 MB → ~33 tiles; assert clamps at 1 and at num_items.
2. Pipelined GET realignment: key *i* returns `-ERR` (WRONGTYPE), key *i+1*
   is a hit — assert per-key results and later payloads intact.
3. Stored value both larger and smaller than `batch_chunk_num_bytes` — both
   drain paths keep the connection usable for a following batch.
4. Multikey-EXISTS partial-count fallback with duplicate keys (existing gap).
5. MGET deprecation warning fires once; `"single"` mode round-trips a
   fully-hit batch correctly.
6. `error_counters()` increments on misses and is cheap when clean.

Build & run (on either the Linux bench host or any box with a C++17
toolchain; CUDA not needed for these):

```bash
uv pip install -e . --no-build-isolation   # in the fork venv
pytest -x tests/v1/storage_backend/test_redis_native_batch_integration.py -q
pytest -x tests/v1/distributed/test_native_connector_l2_adapter.py -q
pytest -q tests/v1/mp_observability/subscribers/metrics/
```

## 6. Microbenchmark acceptance protocol

Prerequisites (already true on the live pair, but verify — results are
meaningless without them):

- Redis Enterprise proxy threads: `sudo /opt/redislabs/bin/rladmin info
  proxy all | grep -i thread` must show 24 (else
  `sudo /opt/redislabs/bin/rladmin tune proxy all threads 24`).
- Record in the results doc: DB type (RAM/Flex), shard count, proxy threads,
  and for Flex the seed/settle protocol (flush → seed → `sleep 90`) —
  tier residency swings Flex results ±15% (see
  `flex_microbenchmark_testing.md` Test 5 notes).

Compare three configs at each sweep point, single client, on **both** the
8-shard 100% RAM DB and the 8-shard Flex DB (RAM-resident keyspace):

- (i) original: `--get-batch-mode single`
- (ii) old default: `--get-batch-mode pipeline --get-min-keys-per-tile 8`
- (iii) new: `--get-batch-mode pipeline --get-target-tile-mb 32`

Sweep points (chosen to bracket the serving stack's real chunk sizes —
chunk64/128/256 ≈ 8.4/16.8/33.6 MB values):

```bash
python benchmarks/storage_backend_io/connector_stage_bench.py \
  --host $REDIS_HOST --port $REDIS_PORT --password $REDIS_PASSWORD \
  --num-workers 64 --batches 8 --warmup 2 \
  --num-keys 125 --value-mb 16.8   # plus: 250 x 8.4, 62 x 33.6, 500 x 4.2
```

Compare `agg_gbps` (wall-clock; the only cross-config comparable number).

Acceptance criteria:

1. (iii) ≥ (ii) at every sweep point, and ≥ (i) everywhere (small
   regressions <5% at a single point are acceptable if flagged).
2. (iii) reaches ≥ 15 GB/s at 125 × 16.8 MB on the RAM DB (the measured
   wire-limited reference is 16.9; 90% of it accounts for run variance).
3. (iii) at 250 × 8.4 MB is within noise of the best of (i)/(ii) — this is
   the point where the old min_keys=8 won, and byte-tiling must not lose it.
4. Stage sanity from the per-batch lines: queue/dispatch/handoff each <2 ms
   at inflight=1 (transfer should be ~all of total).
5. One Flex spilled-keyspace point (flush → seed 500 × 33.6 → sleep 90 →
   `--key-prefix flexspill --skip-set --no-cleanup`) showing (iii) ≥ (ii):
   expect ~6–8.5 GB/s blended; do not chase more — the flash-path node
   ceiling is a known separate issue.

Record everything in a new markdown results file following the format of
`flex_microbenchmark_testing.md` (commands verbatim + full output blocks +
CSV rows).

## 7. Constraints and non-goals

- **Do not** change the `drain_completions()` 4-tuple, the
  `drain_batch_timings()` 8-tuple, `Completion`, or `BatchTiming` — the
  benchmarking harness and the MP adapter depend on them.
- **Do not** remove `get_min_keys_per_tile` (back-compat; deprecate in help).
- **Do not** modify EXISTS/SET/DELETE strategies beyond the shared
  logging/tiling-signature changes.
- New ctor args are appended with defaults (all call sites pass
  positionally: `resp_l2_adapter.py`, `native_clients/resp_client.py`,
  `tests/conftest.py`, integration tests, the microbench).
- Non-goals: the flash-path node ceiling, MP-layer changes, per-key
  streaming completion, upstreaming.

## 8. Deliverables

1. Branch off `stage-timing-instrumentation` (e.g. `pipelined-get-v2`) with
   the changes in §4, tests in §5 passing, clang-format/ruff clean.
2. The microbench results markdown per §6 with a short conclusions section
   stating whether each acceptance criterion was met.
3. A summary of any deviations from this spec and why.
