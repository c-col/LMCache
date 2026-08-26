# SPDX-License-Identifier: Apache-2.0
"""
Integration tests for the batched Redis C++ connector.

These tests exercise the native ``lmcache_redis`` extension's batch paths:

- ``do_batch_exists``: one round trip per tile in either mode — ``pipeline``
  (default: N single-key EXISTS commands written in one batch) or
  ``multikey`` (one multi-key ``EXISTS k1 .. kN``, resolving fully cached /
  fully uncached tiles from its count reply, with a pipelined per-key
  fallback for partially cached tiles).
- ``do_batch_get``: one round trip per tile in either mode — ``pipeline``
  (default: N single-key GETs written in one batch) or ``mget`` (one
  multi-key MGET) — with per-key miss tolerance (nil replies, per-key error
  replies, and size-mismatched values fail only that key and keep the
  connection in protocol sync).
- ``do_batch_set``: one pipelined round trip per tile (N SET commands
  written via scatter-gather, then N status replies).

Requires a running Redis server and the C++ extension. Skipped otherwise.
"""

# Standard
import os
import select
import subprocess
import sys
import time
import uuid

# Third Party
import pytest

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6399"))

CHUNK_SIZE = 4096


def _redis_available() -> bool:
    """Check if Redis is reachable."""
    try:
        result = subprocess.run(
            ["redis-cli", "-h", REDIS_HOST, "-p", str(REDIS_PORT), "ping"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        return result.stdout.strip() == "PONG"
    except Exception:
        return False


def _native_client_available() -> bool:
    """Check if the C++ Redis extension can be imported."""
    try:
        # First Party
        from lmcache.lmcache_redis import LMCacheRedisClient  # noqa: F401

        return True
    except ImportError:
        return False


requires_redis = pytest.mark.skipif(
    not _redis_available(),
    reason=f"Redis not available at {REDIS_HOST}:{REDIS_PORT}",
)
requires_native = pytest.mark.skipif(
    not _native_client_available(),
    reason="C++ Redis extension (lmcache_redis) not available",
)

pytestmark = [requires_redis, requires_native]


def _make_payload(index: int, size: int) -> bytearray:
    """Build a deterministic binary payload of ``size`` bytes.

    The payload embeds CRLF and NUL bytes so that any RESP framing bug in the
    connector (which parses length-prefixed bulk strings) corrupts the data
    visibly instead of passing by luck.
    """
    base = (b"\r\n\x00\xff" + index.to_bytes(4, "big") + b"payload!") * (size // 16 + 1)
    return bytearray(base[:size])


class _SyncClient:
    """Minimal synchronous wrapper over the native client for tests.

    Submits batch operations and blocks on the connector's eventfd until the
    matching completion arrives.
    """

    def __init__(
        self,
        num_workers: int,
        get_min_keys_per_tile: int = 1,
        get_batch_mode: str = "pipeline",
        exists_batch_mode: str = "pipeline",
        get_target_tile_bytes: int = 32_000_000,
    ):
        # First Party
        from lmcache.lmcache_redis import LMCacheRedisClient

        self.client = LMCacheRedisClient(
            REDIS_HOST,
            REDIS_PORT,
            num_workers,
            "",
            "",
            get_min_keys_per_tile,
            get_batch_mode,
            exists_batch_mode,
            get_target_tile_bytes,
        )
        self.poll = select.poll()
        self.poll.register(self.client.event_fd(), select.POLLIN)
        self.pending: dict[int, tuple[bool, str, list[bool]]] = {}
        # future_id -> (op, num_keys, total_bytes, t_submit, t_first_dequeue,
        #               t_first_byte, t_last_done, t_consumed)
        self.timings: dict[int, tuple] = {}

    def _wait(self, future_id: int, timeout_s: float = 10.0):
        if future_id in self.pending:
            return self.pending.pop(future_id)
        while True:
            events = self.poll.poll(timeout_s * 1000)
            if not events:
                raise TimeoutError(f"future {future_id} never completed")
            completions = self.client.drain_completions()
            # timings are co-enqueued with their completion, so draining
            # right after drain_completions() must find one per completion
            t_consumed = time.time()
            for tfid, op, nk, nb, t0, t1, t2, t3 in self.client.drain_batch_timings():
                self.timings[tfid] = (op, nk, nb, t0, t1, t2, t3, t_consumed)
            for fid, ok, err, bools in completions:
                self.pending[fid] = (ok, err, bools)
            if future_id in self.pending:
                return self.pending.pop(future_id)

    def batch_set(self, keys: list[str], payloads: list[bytearray]):
        views = [memoryview(p) for p in payloads]
        return self._wait(self.client.submit_batch_set(keys, views))

    def batch_get(self, keys: list[str], bufs: list[bytearray]):
        views = [memoryview(b) for b in bufs]
        return self._wait(self.client.submit_batch_get(keys, views))

    def batch_exists(self, keys: list[str]):
        return self._wait(self.client.submit_batch_exists(keys))

    def batch_delete(self, keys: list[str]):
        return self._wait(self.client.submit_batch_delete(keys))

    def batch_set_timed(self, keys: list[str], payloads: list[bytearray]):
        """Like batch_set, but also returns the batch's timing record."""
        views = [memoryview(p) for p in payloads]
        fid = self.client.submit_batch_set(keys, views)
        result = self._wait(fid)
        return result, self.timings.pop(fid, None)

    def batch_get_timed(self, keys: list[str], bufs: list[bytearray]):
        """Like batch_get, but also returns the batch's timing record."""
        views = [memoryview(b) for b in bufs]
        fid = self.client.submit_batch_get(keys, views)
        result = self._wait(fid)
        return result, self.timings.pop(fid, None)

    def batch_exists_timed(self, keys: list[str]):
        """Like batch_exists, but also returns the batch's timing record."""
        fid = self.client.submit_batch_exists(keys)
        result = self._wait(fid)
        return result, self.timings.pop(fid, None)

    def close(self):
        self.client.close()


# (get_batch_mode, exists_batch_mode) pairs covering both miss-tolerant wire
# strategy stacks: the cluster-safe pipelined default and the multi-key
# alternative. The "single" GET mode is deliberately NOT in this list: it is
# a benchmarking baseline whose misses desync the connection, so it only runs
# in TestSingleMode against fully-hit keyspaces.
MODE_PAIRS = [("pipeline", "pipeline"), ("mget", "multikey")]


@pytest.fixture(params=MODE_PAIRS, ids=["pipeline", "multikey"])
def client(request):
    """Single-worker client: every tile shares one connection, so any RESP
    desync from a previous operation breaks the next one loudly.

    Parametrized over both batch-mode stacks so every test exercises both
    wire strategies.
    """
    get_mode, exists_mode = request.param
    c = _SyncClient(
        num_workers=1, get_batch_mode=get_mode, exists_batch_mode=exists_mode
    )
    yield c
    c.close()


@pytest.fixture
def prefix():
    """Unique key prefix per test to avoid collisions across runs."""
    return f"lmcache-batch-test:{uuid.uuid4().hex[:12]}"


@pytest.fixture
def stored_keys(client, prefix):
    """Eight chunk-sized keys stored in Redis, deleted on teardown."""
    keys = [f"{prefix}:k{i}" for i in range(8)]
    payloads = [_make_payload(i, CHUNK_SIZE) for i in range(8)]
    ok, err, _ = client.batch_set(keys, payloads)
    assert ok, err
    yield keys, payloads
    client.batch_delete(keys)


def _missing_keys(prefix: str, count: int) -> list[str]:
    return [f"{prefix}:missing{i}" for i in range(count)]


class TestBatchExists:
    def test_all_exist_fast_path(self, client, stored_keys):
        keys, _ = stored_keys
        ok, err, results = client.batch_exists(keys)
        assert ok, err
        assert results == [True] * len(keys)

    def test_none_exist_fast_path(self, client, prefix):
        ok, err, results = client.batch_exists(_missing_keys(prefix, 8))
        assert ok, err
        assert results == [False] * 8

    def test_partial_uses_pipelined_fallback(self, client, stored_keys, prefix):
        keys, _ = stored_keys
        missing = _missing_keys(prefix, 3)
        mixed = [keys[0], missing[0], keys[1], missing[1], missing[2], keys[2]]
        ok, err, results = client.batch_exists(mixed)
        assert ok, err
        assert results == [True, False, True, False, False, True]

    def test_duplicate_keys(self, client, stored_keys, prefix):
        keys, _ = stored_keys
        missing = _missing_keys(prefix, 1)

        ok, _, results = client.batch_exists([keys[0], keys[0]])
        assert ok and results == [True, True]

        ok, _, results = client.batch_exists([missing[0], missing[0]])
        assert ok and results == [False, False]

        # duplicates in a partial batch must resolve per position
        ok, _, results = client.batch_exists([keys[0], missing[0], keys[0]])
        assert ok and results == [True, False, True]

    def test_single_key(self, client, stored_keys, prefix):
        keys, _ = stored_keys
        ok, _, results = client.batch_exists([keys[0]])
        assert ok and results == [True]
        ok, _, results = client.batch_exists(_missing_keys(prefix, 1))
        assert ok and results == [False]


class TestBatchGet:
    def test_all_hit(self, client, stored_keys):
        keys, payloads = stored_keys
        bufs = [bytearray(CHUNK_SIZE) for _ in keys]
        ok, err, results = client.batch_get(keys, bufs)
        assert ok, err
        assert results == [True] * len(keys)
        assert bufs == payloads

    def test_mixed_hit_miss(self, client, stored_keys, prefix):
        keys, payloads = stored_keys
        missing = _missing_keys(prefix, 3)
        mixed = [missing[0], keys[0], missing[1], keys[1], keys[2], missing[2]]
        bufs = [bytearray(CHUNK_SIZE) for _ in mixed]

        ok, err, results = client.batch_get(mixed, bufs)
        assert ok, err
        assert results == [False, True, False, True, True, False]
        assert bufs[1] == payloads[0]
        assert bufs[3] == payloads[1]
        assert bufs[4] == payloads[2]
        # missed keys must leave their buffers untouched
        assert bufs[0] == bytearray(CHUNK_SIZE)
        assert bufs[2] == bytearray(CHUNK_SIZE)

    def test_all_miss(self, client, prefix):
        missing = _missing_keys(prefix, 4)
        bufs = [bytearray(CHUNK_SIZE) for _ in missing]
        ok, err, results = client.batch_get(missing, bufs)
        assert ok, err
        assert results == [False] * 4

    def test_size_mismatch_keeps_connection_in_sync(self, client, stored_keys, prefix):
        keys, payloads = stored_keys
        small_key = f"{prefix}:small"
        ok, err, _ = client.batch_set([small_key], [_make_payload(99, 128)])
        assert ok, err

        bufs = [bytearray(CHUNK_SIZE) for _ in range(3)]
        ok, err, results = client.batch_get([keys[0], small_key, keys[1]], bufs)
        assert ok, err
        assert results == [True, False, True]
        assert bufs[0] == payloads[0]
        assert bufs[2] == payloads[1]

        # the mismatched value must be fully drained: the same connection
        # (single worker) must keep working
        ok, err, results = client.batch_exists(keys)
        assert ok, err
        assert results == [True] * len(keys)

        client.batch_delete([small_key])

    def test_oversized_value_keeps_connection_in_sync(
        self, client, stored_keys, prefix
    ):
        """A stored value LARGER than the destination buffer must be fully
        drained (the smaller-value case is covered above); the connection
        stays usable for the surrounding keys and a following batch."""
        keys, payloads = stored_keys
        big_key = f"{prefix}:big"
        ok, err, _ = client.batch_set([big_key], [_make_payload(98, CHUNK_SIZE * 2)])
        assert ok, err

        bufs = [bytearray(CHUNK_SIZE) for _ in range(3)]
        ok, err, results = client.batch_get([keys[0], big_key, keys[1]], bufs)
        assert ok, err
        assert results == [True, False, True]
        assert bufs[0] == payloads[0]
        assert bufs[2] == payloads[1]
        assert bufs[1] == bytearray(CHUNK_SIZE)

        # the oversized value must be fully drained: the same connection
        # (single worker) must keep working
        ok, err, results = client.batch_exists(keys)
        assert ok, err
        assert results == [True] * len(keys)

        client.batch_delete([big_key])

    def test_error_counters(self, client, stored_keys, prefix):
        """error_counters(): zero on a clean client, then counts misses and
        size mismatches; cheap enough to poll after every batch."""
        keys, payloads = stored_keys
        zero = {
            "get_not_found": 0,
            "get_size_mismatch": 0,
            "get_error_reply": 0,
            "delete_failed": 0,
        }
        assert client.client.error_counters() == zero

        bufs = [bytearray(CHUNK_SIZE) for _ in keys]
        ok, err, _ = client.batch_get(keys, bufs)
        assert ok, err
        assert client.client.error_counters() == zero

        missing = _missing_keys(prefix, 3)
        bufs = [bytearray(CHUNK_SIZE) for _ in missing]
        ok, err, _ = client.batch_get(missing, bufs)
        assert ok, err
        counters = client.client.error_counters()
        assert counters["get_not_found"] == 3
        assert counters["get_size_mismatch"] == 0

        small_key = f"{prefix}:ctrsmall"
        ok, err, _ = client.batch_set([small_key], [_make_payload(97, 128)])
        assert ok, err
        try:
            bufs = [bytearray(CHUNK_SIZE)]
            ok, err, _ = client.batch_get([small_key], bufs)
            assert ok, err
            counters = client.client.error_counters()
            assert counters["get_not_found"] == 3  # monotone, unchanged
            assert counters["get_size_mismatch"] == 1
        finally:
            client.batch_delete([small_key])

    def test_wrong_type_key_fails_per_key(self, client, stored_keys, prefix):
        """A non-string key fails only itself: pipeline mode gets a per-key
        WRONGTYPE error reply, mget mode gets a nil — both must keep the
        connection in sync for the surrounding keys and later batches."""
        keys, payloads = stored_keys
        list_key = f"{prefix}:wrongtype"
        subprocess.run(
            [
                "redis-cli",
                "-h",
                REDIS_HOST,
                "-p",
                str(REDIS_PORT),
                "lpush",
                list_key,
                "x",
            ],
            capture_output=True,
            timeout=5,
            check=True,
        )

        try:
            bufs = [bytearray(CHUNK_SIZE) for _ in range(3)]
            ok, err, results = client.batch_get([keys[0], list_key, keys[1]], bufs)
            assert ok, err
            assert results == [True, False, True]
            assert bufs[0] == payloads[0]
            assert bufs[2] == payloads[1]

            # connection must still be usable after the error reply
            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == [True] * len(keys)
        finally:
            client.batch_delete([list_key])


class TestBatchTiling:
    """Batches larger than the worker count are split into per-worker tiles;
    each tile issues its own MGET / multi-key EXISTS."""

    NUM_KEYS = 200
    BIG_CHUNK = 16384

    @pytest.fixture(params=MODE_PAIRS, ids=["pipeline", "multikey"])
    def multi_worker_client(self, request):
        get_mode, exists_mode = request.param
        # tiny byte target: these values are far below the 32 MB default, so
        # without it the whole batch would collapse into one tile and stop
        # exercising the multi-tile paths this class is about
        c = _SyncClient(
            num_workers=8,
            get_batch_mode=get_mode,
            exists_batch_mode=exists_mode,
            get_target_tile_bytes=1,
        )
        yield c
        c.close()

    def test_large_partial_batch(self, multi_worker_client, prefix):
        client = multi_worker_client
        keys = [f"{prefix}:big{i}" for i in range(self.NUM_KEYS)]
        payloads = [_make_payload(i, self.BIG_CHUNK) for i in range(self.NUM_KEYS)]

        ok, err, _ = client.batch_set(keys, payloads)
        assert ok, err

        try:
            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == [True] * self.NUM_KEYS

            # delete every 3rd key so every tile sees a partial mix
            ok, err, _ = client.batch_delete(keys[::3])
            assert ok, err
            expected = [i % 3 != 0 for i in range(self.NUM_KEYS)]

            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == expected

            bufs = [bytearray(self.BIG_CHUNK) for _ in keys]
            ok, err, results = client.batch_get(keys, bufs)
            assert ok, err
            assert results == expected
            for i in range(self.NUM_KEYS):
                if expected[i]:
                    assert bufs[i] == payloads[i]
                else:
                    assert bufs[i] == bytearray(self.BIG_CHUNK)
        finally:
            client.batch_delete(keys)

    def test_concurrent_batches(self, multi_worker_client, prefix):
        client = multi_worker_client
        keys = [f"{prefix}:conc{i}" for i in range(50)]
        payloads = [_make_payload(i, self.BIG_CHUNK) for i in range(50)]
        ok, err, _ = client.batch_set(keys, payloads)
        assert ok, err

        try:
            future_ids = []
            buf_sets = []
            for _ in range(6):
                bufs = [bytearray(self.BIG_CHUNK) for _ in keys]
                buf_sets.append(bufs)
                future_ids.append(
                    client.client.submit_batch_get(keys, [memoryview(b) for b in bufs])
                )
                future_ids.append(client.client.submit_batch_exists(keys))

            outcomes = [client._wait(fid) for fid in future_ids]
            assert all(ok for ok, _, _ in outcomes)
            for ok, _, results in outcomes:
                assert results == [True] * 50
            for bufs in buf_sets:
                assert bufs == payloads
        finally:
            client.batch_delete(keys)


class TestTilingShapes:
    """Regression tests for batch shapes whose ceil-based tiling used to
    produce empty tiles (which emitted argument-less MGET/EXISTS commands and
    failed the whole batch with -ERR) and degenerate 1-key remainder tiles.

    ``get_min_keys_per_tile=1`` plus a 1-byte tile target forces maximum GET
    fan-out so the partition logic itself is exercised rather than collapsed
    into a single tile (these chunk-sized values are far below the 32 MB
    byte-tiling default).
    """

    # (num_keys, num_workers) shapes where ceil-splitting overshot:
    # 9/8 -> 5 tiles of 2 + 3 EMPTY, 13/6 -> 3,3,3,3,1 + 1 EMPTY,
    # 10/8 -> 5 tiles of 2 + 3 EMPTY, 17/16 -> 9 tiles of 2 + 7 EMPTY
    SHAPES = [(9, 8), (13, 6), (10, 8), (17, 16)]

    @pytest.mark.parametrize(
        "get_batch_mode,exists_batch_mode", MODE_PAIRS, ids=["pipeline", "multikey"]
    )
    @pytest.mark.parametrize("num_keys,num_workers", SHAPES)
    def test_no_empty_tiles(
        self, prefix, num_keys, num_workers, get_batch_mode, exists_batch_mode
    ):
        client = _SyncClient(
            num_workers=num_workers,
            get_min_keys_per_tile=1,
            get_batch_mode=get_batch_mode,
            exists_batch_mode=exists_batch_mode,
            get_target_tile_bytes=1,
        )
        keys = [f"{prefix}:shape{i}" for i in range(num_keys)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(num_keys)]

        try:
            ok, err, _ = client.batch_set(keys, payloads)
            assert ok, err

            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == [True] * num_keys

            bufs = [bytearray(CHUNK_SIZE) for _ in keys]
            ok, err, results = client.batch_get(keys, bufs)
            assert ok, err
            assert results == [True] * num_keys
            assert bufs == payloads

            ok, err, results = client.batch_delete(keys)
            assert ok, err
            assert results == [True] * num_keys
        finally:
            client.close()

    @pytest.mark.parametrize(
        "get_batch_mode,exists_batch_mode", MODE_PAIRS, ids=["pipeline", "multikey"]
    )
    @pytest.mark.parametrize("num_keys,num_workers", SHAPES)
    def test_no_empty_tiles_when_partially_cached(
        self, prefix, num_keys, num_workers, get_batch_mode, exists_batch_mode
    ):
        client = _SyncClient(
            num_workers=num_workers,
            get_min_keys_per_tile=1,
            get_batch_mode=get_batch_mode,
            exists_batch_mode=exists_batch_mode,
            get_target_tile_bytes=1,
        )
        keys = [f"{prefix}:pshape{i}" for i in range(num_keys)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(num_keys)]

        try:
            ok, err, _ = client.batch_set(keys, payloads)
            assert ok, err
            ok, err, _ = client.batch_delete(keys[::2])
            assert ok, err
            expected = [i % 2 != 0 for i in range(num_keys)]

            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == expected

            bufs = [bytearray(CHUNK_SIZE) for _ in keys]
            ok, err, results = client.batch_get(keys, bufs)
            assert ok, err
            assert results == expected
        finally:
            client.batch_delete(keys)
            client.close()


def _command_calls(command: str) -> int:
    """Return the cumulative call count for a Redis command from commandstats.

    Assumes a dedicated test server (as the rest of this module does); other
    concurrent clients issuing the same command would skew the delta-based
    assertions below.
    """
    out = subprocess.run(
        ["redis-cli", "-h", REDIS_HOST, "-p", str(REDIS_PORT), "info", "commandstats"],
        capture_output=True,
        text=True,
        timeout=5,
    ).stdout
    for line in out.splitlines():
        if line.startswith(f"cmdstat_{command}:"):
            return int(line.split("calls=")[1].split(",")[0])
    return 0


class TestTilingPolicy:
    """Verify the command counts produced by RedisConnector::choose_num_tiles:
    EXISTS collapses to one multi-key command, deprecated mget-mode GET still
    splits by the get_min_keys_per_tile floor. (Pipeline-mode tile counts are
    not observable from command counts — one GET per key regardless — so
    byte-based tiling is asserted via _plan_get_tiles in TestByteTiling.)"""

    def test_exists_pipeline_mode_uses_per_key_commands(self, prefix):
        """Pipeline mode (default): N single-key EXISTS commands in one
        round trip, regardless of hit pattern."""
        client = _SyncClient(num_workers=8)
        keys = [f"{prefix}:pol{i}" for i in range(9)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(9)]

        try:
            ok, err, _ = client.batch_set(keys, payloads)
            assert ok, err
            ok, err, _ = client.batch_delete(keys[:4])
            assert ok, err

            before = _command_calls("exists")
            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == [False] * 4 + [True] * 5
            assert _command_calls("exists") - before == 9
        finally:
            client.batch_delete(keys)
            client.close()

    def test_exists_multikey_fully_cached_uses_one_command(self, prefix):
        client = _SyncClient(num_workers=8, exists_batch_mode="multikey")
        keys = [f"{prefix}:polm{i}" for i in range(9)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(9)]

        try:
            ok, err, _ = client.batch_set(keys, payloads)
            assert ok, err

            before = _command_calls("exists")
            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == [True] * 9
            # fully cached: one multi-key EXISTS, no pipelined fallback
            assert _command_calls("exists") - before == 1
        finally:
            client.batch_delete(keys)
            client.close()

    def test_exists_multikey_partial_pipelines_per_key(self, prefix):
        client = _SyncClient(num_workers=8, exists_batch_mode="multikey")
        keys = [f"{prefix}:polp{i}" for i in range(9)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(9)]

        try:
            ok, err, _ = client.batch_set(keys, payloads)
            assert ok, err
            ok, err, _ = client.batch_delete(keys[:4])
            assert ok, err

            before = _command_calls("exists")
            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == [False] * 4 + [True] * 5
            # partial: 1 multi-key EXISTS + 9 pipelined single-key EXISTS
            assert _command_calls("exists") - before == 10
        finally:
            client.batch_delete(keys)
            client.close()

    @pytest.mark.parametrize(
        "num_keys,min_keys_per_tile,expected_mgets",
        [
            (8, 8, 1),  # exactly one floor's worth -> single MGET
            (32, 8, 4),  # ceil(32/8) = 4 tiles
            (32, 100, 1),  # floor larger than batch -> single MGET
            (9, 1, 8),  # floor 1 -> one tile per worker (8 workers)
        ],
    )
    def test_mget_respects_min_keys_per_tile(
        self, prefix, num_keys, min_keys_per_tile, expected_mgets
    ):
        client = _SyncClient(
            num_workers=8,
            get_min_keys_per_tile=min_keys_per_tile,
            get_batch_mode="mget",
        )
        keys = [f"{prefix}:polm{i}" for i in range(num_keys)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(num_keys)]

        try:
            ok, err, _ = client.batch_set(keys, payloads)
            assert ok, err

            bufs = [bytearray(CHUNK_SIZE) for _ in keys]
            before = _command_calls("mget")
            ok, err, results = client.batch_get(keys, bufs)
            assert ok, err
            assert results == [True] * num_keys
            assert bufs == payloads
            assert _command_calls("mget") - before == expected_mgets
        finally:
            client.batch_delete(keys)
            client.close()

    def test_exists_multikey_duplicates_fully_cached(self, prefix):
        """Duplicate keys in a fully-cached multikey batch: EXISTS counts
        each argument position independently, so count == N still resolves
        the batch in ONE command with per-position True results."""
        client = _SyncClient(num_workers=8, exists_batch_mode="multikey")
        keys = [f"{prefix}:pold{i}" for i in range(3)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(3)]

        try:
            ok, err, _ = client.batch_set(keys, payloads)
            assert ok, err

            batch = [keys[0], keys[1], keys[0], keys[2], keys[1]]
            before = _command_calls("exists")
            ok, err, results = client.batch_exists(batch)
            assert ok, err
            assert results == [True] * 5
            # count == 5 == num_keys: fast path, no pipelined fallback
            assert _command_calls("exists") - before == 1
        finally:
            client.batch_delete(keys)
            client.close()

    def test_pipeline_mode_uses_single_key_gets(self, prefix):
        """Pipeline mode issues N single-key GETs (one round trip per tile)
        and no MGET commands."""
        num_keys = 32
        client = _SyncClient(
            num_workers=8,
            get_min_keys_per_tile=8,
            get_batch_mode="pipeline",
        )
        keys = [f"{prefix}:polg{i}" for i in range(num_keys)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(num_keys)]

        try:
            ok, err, _ = client.batch_set(keys, payloads)
            assert ok, err

            bufs = [bytearray(CHUNK_SIZE) for _ in keys]
            gets_before = _command_calls("get")
            mgets_before = _command_calls("mget")
            ok, err, results = client.batch_get(keys, bufs)
            assert ok, err
            assert results == [True] * num_keys
            assert bufs == payloads
            assert _command_calls("get") - gets_before == num_keys
            assert _command_calls("mget") - mgets_before == 0
        finally:
            client.batch_delete(keys)
            client.close()


@pytest.fixture(scope="module")
def planner():
    """64-worker client used only for _plan_get_tiles queries (module-scoped:
    64 connections are worth reusing across the parametrized cases)."""
    c = _SyncClient(num_workers=64)
    yield c.client
    c.close()


class TestByteTiling:
    """Byte-based tile sizing for pipelined GET, asserted via the
    _plan_get_tiles test hook (pipeline tiling is invisible in command
    counts: one GET per key regardless of how keys are grouped into
    tiles/sockets). Expected values mirror the measured sweep points in the
    microbenchmark protocol (chunk64/128/256 ~= 8.4/16.8/33.6 MB values)."""

    MB = 1000 * 1000

    @pytest.mark.parametrize(
        "num_items,value_bytes,expected_tiles",
        [
            (125, int(16.8 * MB), 64),  # ceil(2100/32)=66, capped at workers
            (250, int(8.4 * MB), 64),  # ceil(2100/32)=66, capped at workers
            (62, int(33.6 * MB), 62),  # ceil(2083/32)=66, capped at num_items
            (250, int(4.2 * MB), 33),  # ceil(1050/32)
            (500, int(4.2 * MB), 64),  # ceil(2100/32)=66, capped at workers
            (2, 100 * MB, 2),  # capped at num_items
            (3, 4096, 1),  # tiny batch -> 1 tile
            (1, 1, 1),  # lower clamp
        ],
    )
    def test_tiles_by_bytes(self, planner, num_items, value_bytes, expected_tiles):
        assert planner._plan_get_tiles(num_items, value_bytes) == expected_tiles

    def test_explicit_min_keys_caps_fanout(self):
        """Back-compat: an explicitly set get_min_keys_per_tile > 1 still
        caps the byte-based tile count."""
        c = _SyncClient(num_workers=64, get_min_keys_per_tile=8)
        try:
            # byte target alone would pick 64; min_keys=8 caps at ceil(125/8)
            assert c.client._plan_get_tiles(125, int(16.8 * self.MB)) == 16
            # min_keys never pushes the count ABOVE the byte-based choice
            assert c.client._plan_get_tiles(3, 4096) == 1
        finally:
            c.close()

    def test_custom_target_tile_bytes(self):
        c = _SyncClient(num_workers=64, get_target_tile_bytes=8 * self.MB)
        try:
            # ceil(10 * 4.2 MB / 8 MB) = 6
            assert c.client._plan_get_tiles(10, int(4.2 * self.MB)) == 6
        finally:
            c.close()

    def test_mget_mode_keeps_key_count_tiling(self):
        """Deprecated mget mode is untouched by the byte target: it still
        tiles by the get_min_keys_per_tile floor."""
        c = _SyncClient(num_workers=8, get_min_keys_per_tile=8, get_batch_mode="mget")
        try:
            assert c.client._plan_get_tiles(32, int(16.8 * self.MB)) == 4
        finally:
            c.close()


class TestSingleMode:
    """get_batch_mode="single": upstream LMCache's original one-blocking-
    round-trip-per-key GET, kept as a benchmarking baseline. Only exercised
    against fully-hit keyspaces — a miss desyncs the connection by design
    (do_single_get memcmps the size header), which is exactly the limitation
    the batched modes exist to fix."""

    @pytest.fixture
    def single_client(self):
        c = _SyncClient(num_workers=2, get_batch_mode="single")
        yield c
        c.close()

    def test_fully_hit_round_trip(self, single_client, prefix):
        client = single_client
        keys = [f"{prefix}:single{i}" for i in range(10)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(10)]

        ok, err, _ = client.batch_set(keys, payloads)
        assert ok, err
        try:
            bufs = [bytearray(CHUNK_SIZE) for _ in keys]
            ok, err, results = client.batch_get(keys, bufs)
            assert ok, err
            assert results == [True] * len(keys)
            assert bufs == payloads

            ok, err, results = client.batch_exists(keys)
            assert ok, err
            assert results == [True] * len(keys)
        finally:
            ok, err, results = client.batch_delete(keys)
            assert ok, err
            assert results == [True] * len(keys)

    def test_uses_per_key_get_commands(self, single_client, prefix):
        client = single_client
        keys = [f"{prefix}:singlecmd{i}" for i in range(6)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(6)]
        ok, err, _ = client.batch_set(keys, payloads)
        assert ok, err
        try:
            bufs = [bytearray(CHUNK_SIZE) for _ in keys]
            gets_before = _command_calls("get")
            mgets_before = _command_calls("mget")
            ok, err, results = client.batch_get(keys, bufs)
            assert ok, err
            assert results == [True] * 6
            assert _command_calls("get") - gets_before == 6
            assert _command_calls("mget") - mgets_before == 0
        finally:
            client.batch_delete(keys)

    def test_get_timing_has_first_byte(self, single_client, prefix):
        """Single mode gets its first-byte stamp from the base-loop hook
        (after the first key's round trip)."""
        client = single_client
        keys = [f"{prefix}:singletim{i}" for i in range(4)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(4)]
        ok, err, _ = client.batch_set(keys, payloads)
        assert ok, err
        try:
            bufs = [bytearray(CHUNK_SIZE) for _ in keys]
            (ok, err, _), timing = client.batch_get_timed(keys, bufs)
            assert ok, err
            assert timing is not None
            op, num_keys, total_bytes, t_submit, t_dequeue, t_first_byte, t_last, _ = (
                timing
            )
            assert op == "get"
            assert num_keys == 4
            assert total_bytes == 4 * CHUNK_SIZE
            assert t_first_byte > 0.0
            assert t_submit <= t_dequeue <= t_first_byte <= t_last
        finally:
            client.batch_delete(keys)


class TestMgetDeprecation:
    def test_warning_fires_once_per_process(self):
        """The mget deprecation warning is once-per-process, and this test
        session has already constructed mget clients (MODE_PAIRS), so assert
        in a fresh subprocess: two mget clients -> exactly one warning."""
        script = (
            "from lmcache.lmcache_redis import LMCacheRedisClient\n"
            f"a = LMCacheRedisClient({REDIS_HOST!r}, {REDIS_PORT}, 1, '', '', "
            "1, 'mget')\n"
            f"b = LMCacheRedisClient({REDIS_HOST!r}, {REDIS_PORT}, 1, '', '', "
            "1, 'mget')\n"
            "a.close(); b.close()\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        warnings = [
            line
            for line in result.stderr.splitlines()
            if "get_batch_mode=mget is deprecated" in line
        ]
        assert len(warnings) == 1, result.stderr


class TestBatchTimings:
    """drain_batch_timings(): one record per batch with monotonic,
    wall-clock-aligned stage timestamps."""

    def test_get_timing_monotonic_and_matches(self, client, stored_keys, prefix):
        keys, payloads = stored_keys
        bufs = [bytearray(CHUNK_SIZE) for _ in keys]

        t0 = time.time() - 0.5
        (ok, err, bools), timing = client.batch_get_timed(keys, bufs)
        t1 = time.time() + 0.5
        assert ok, err

        assert timing is not None, "no timing record for GET batch"
        (op, num_keys, total_bytes, t_submit, t_dequeue, t_first_byte,
         t_last_done, t_consumed) = timing
        assert op == "get"
        assert num_keys == len(keys)
        assert total_bytes == len(keys) * CHUNK_SIZE
        # monotonic within the connector's anchored clock
        assert t_submit <= t_dequeue <= t_last_done
        # GET reports a first byte between dequeue and completion
        assert t_first_byte > 0.0
        assert t_dequeue <= t_first_byte <= t_last_done
        # aligned with Python time.time()
        for ts in (t_submit, t_dequeue, t_first_byte, t_last_done):
            assert t0 <= ts <= t1
        assert t_consumed >= t0

    def test_set_timing_has_no_first_byte(self, client, prefix):
        keys = [f"{prefix}:timed{i}" for i in range(4)]
        payloads = [_make_payload(i, CHUNK_SIZE) for i in range(4)]
        try:
            (ok, err, _), timing = client.batch_set_timed(keys, payloads)
            assert ok, err
            assert timing is not None, "no timing record for SET batch"
            (op, num_keys, total_bytes, t_submit, t_dequeue, t_first_byte,
             t_last_done, _) = timing
            assert op == "set"
            assert num_keys == 4
            assert total_bytes == 4 * CHUNK_SIZE
            assert t_submit <= t_dequeue <= t_last_done
            # SET does not report a first byte (v1: GET only)
            assert t_first_byte == 0.0
        finally:
            client.batch_delete(keys)

    def test_exists_timing_zero_bytes(self, client, stored_keys):
        keys, _ = stored_keys
        (ok, err, bools), timing = client.batch_exists_timed(keys)
        assert ok, err
        assert timing is not None, "no timing record for EXISTS batch"
        op, num_keys, total_bytes, t_submit, t_dequeue, _, t_last_done, _ = timing
        assert op == "exists"
        assert num_keys == len(keys)
        assert total_bytes == 0
        assert t_submit <= t_dequeue <= t_last_done

    def test_second_drain_returns_empty(self, client, stored_keys):
        keys, _ = stored_keys
        (ok, err, _), timing = client.batch_exists_timed(keys)
        assert ok, err
        assert timing is not None
        # every record was already drained by _wait
        assert client.client.drain_batch_timings() == []
        # NOTE: client.timings is NOT empty here — the stored_keys fixture's
        # plain batch_set() drained its own SET record into the dict and
        # nothing pops it (only the *_timed helpers pop their future_id).
        # The connector-side contract is the empty drain asserted above.
        assert all(t[0] == "set" for t in client.timings.values())
