# SPDX-License-Identifier: Apache-2.0
"""
Integration tests for the batched Redis C++ connector (multi-key EXISTS and
MGET).

These tests exercise the native ``lmcache_redis`` extension's batch paths:

- ``do_batch_exists``: one multi-key ``EXISTS k1 .. kN`` round trip when the
  tile is fully cached or fully uncached, with a pipelined per-key fallback
  for partially cached tiles.
- ``do_batch_get``: one ``MGET k1 .. kN`` round trip per tile with per-key
  miss tolerance (``$-1`` nil replies and size-mismatched values fail only
  that key and keep the connection in protocol sync).

Requires a running Redis server and the C++ extension. Skipped otherwise.
"""

# Standard
import os
import select
import subprocess
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

    def __init__(self, num_workers: int, mget_min_keys_per_tile: int = 8):
        # First Party
        from lmcache.lmcache_redis import LMCacheRedisClient

        self.client = LMCacheRedisClient(
            REDIS_HOST,
            REDIS_PORT,
            num_workers,
            "",
            "",
            mget_min_keys_per_tile,
        )
        self.poll = select.poll()
        self.poll.register(self.client.event_fd(), select.POLLIN)
        self.pending: dict[int, tuple[bool, str, list[bool]]] = {}

    def _wait(self, future_id: int, timeout_s: float = 10.0):
        if future_id in self.pending:
            return self.pending.pop(future_id)
        while True:
            events = self.poll.poll(timeout_s * 1000)
            if not events:
                raise TimeoutError(f"future {future_id} never completed")
            for fid, ok, err, bools in self.client.drain_completions():
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

    def close(self):
        self.client.close()


@pytest.fixture
def client():
    """Single-worker client: every tile shares one connection, so any RESP
    desync from a previous operation breaks the next one loudly."""
    c = _SyncClient(num_workers=1)
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


class TestBatchTiling:
    """Batches larger than the worker count are split into per-worker tiles;
    each tile issues its own MGET / multi-key EXISTS."""

    NUM_KEYS = 200
    BIG_CHUNK = 16384

    @pytest.fixture
    def multi_worker_client(self):
        c = _SyncClient(num_workers=8)
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

    ``mget_min_keys_per_tile=1`` forces maximum GET fan-out so the partition
    logic itself is exercised rather than collapsed into a single tile.
    """

    # (num_keys, num_workers) shapes where ceil-splitting overshot:
    # 9/8 -> 5 tiles of 2 + 3 EMPTY, 13/6 -> 3,3,3,3,1 + 1 EMPTY,
    # 10/8 -> 5 tiles of 2 + 3 EMPTY, 17/16 -> 9 tiles of 2 + 7 EMPTY
    SHAPES = [(9, 8), (13, 6), (10, 8), (17, 16)]

    @pytest.mark.parametrize("num_keys,num_workers", SHAPES)
    def test_no_empty_tiles(self, prefix, num_keys, num_workers):
        client = _SyncClient(num_workers=num_workers, mget_min_keys_per_tile=1)
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

    @pytest.mark.parametrize("num_keys,num_workers", SHAPES)
    def test_no_empty_tiles_when_partially_cached(self, prefix, num_keys, num_workers):
        client = _SyncClient(num_workers=num_workers, mget_min_keys_per_tile=1)
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
    EXISTS collapses to one multi-key command, GET splits by the
    mget_min_keys_per_tile floor."""

    def test_exists_uses_one_command(self, prefix):
        client = _SyncClient(num_workers=8)
        keys = [f"{prefix}:pol{i}" for i in range(9)]
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

    def test_exists_partial_pipelines_per_key(self, prefix):
        client = _SyncClient(num_workers=8)
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
        client = _SyncClient(num_workers=8, mget_min_keys_per_tile=min_keys_per_tile)
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
