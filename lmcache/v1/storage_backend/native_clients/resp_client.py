# SPDX-License-Identifier: Apache-2.0
# Standard
from typing import Optional
import asyncio

# Local
from .connector_client_base import ConnectorClientBase

try:
    # First Party
    from lmcache.lmcache_redis import LMCacheRedisClient

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    LMCacheRedisClient = None  # type: ignore


class RESPClient(ConnectorClientBase[LMCacheRedisClient]):
    def __init__(
        self,
        host: str,
        port: int,
        num_workers: int,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        username: str = "",
        password: str = "",
        get_min_keys_per_tile: int = 1,
        get_batch_mode: str = "pipeline",
        exists_batch_mode: str = "pipeline",
        get_target_tile_bytes: int = 32_000_000,
    ):
        """Create a RESP client backed by the native C++ Redis connector.

        Args:
            host: Redis/Valkey server hostname or IP.
            port: server port.
            num_workers: C++ worker threads (each with its own connection).
            loop: asyncio event loop for completion dispatch (defaults to the
                running loop).
            username: optional auth username.
            password: optional auth password.
            get_min_keys_per_tile: DEPRECATED in favor of
                get_target_tile_bytes. Minimum keys a batched GET tile
                carries before the batch is split across more worker
                connections (must be >= 1; the default of 1 has no effect,
                values > 1 cap the byte-based tile count).
            get_batch_mode: how a batched-GET tile is executed. "pipeline"
                (default and only supported batched mode) writes N single-key
                GET commands in one batch — cluster-safe and fair to
                co-tenant clients. "mget" (deprecated) issues one multi-key
                MGET per tile — single node/proxy only. "single" issues one
                blocking round trip per key — benchmarking baseline only; a
                miss desyncs the connection.
            exists_batch_mode: how a batched-EXISTS tile is executed.
                "pipeline" (default) writes N single-key EXISTS commands in
                one batch — per-key results in one round trip, cluster-safe.
                "multikey" issues one multi-key EXISTS per tile with a
                pipelined per-key fallback for partial hits — single
                node/proxy only.
            get_target_tile_bytes: target payload bytes per pipelined-GET
                tile; tile count ~= ceil(batch_bytes / target), clamped to
                [1, num_workers] (must be >= 1).

        Raises:
            RuntimeError: if the C++ Redis extension is not built, if
                get_min_keys_per_tile or get_target_tile_bytes is < 1, if
                get_batch_mode is not "pipeline", "mget", or "single", or if
                exists_batch_mode is not "pipeline" or "multikey".
        """
        if not REDIS_AVAILABLE:
            raise RuntimeError(
                "RESPClient requires the C++ Redis extension. "
                "Build with: pip install -e ."
            )
        native_client = LMCacheRedisClient(
            host,
            port,
            num_workers,
            username,
            password,
            get_min_keys_per_tile,
            get_batch_mode,
            exists_batch_mode,
            get_target_tile_bytes,
        )
        super().__init__(native_client, loop)
