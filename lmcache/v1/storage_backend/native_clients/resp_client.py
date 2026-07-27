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
        get_min_keys_per_tile: int = 8,
        get_batch_mode: str = "pipeline",
        exists_batch_mode: str = "pipeline",
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
            get_min_keys_per_tile: minimum keys a batched GET tile carries
                before the batch is split across more worker connections
                (must be >= 1; applies to both get_batch_mode settings).
                Higher values favor fewer, larger batched commands; lower
                values favor connection-level parallelism.
            get_batch_mode: how a batched-GET tile is executed. "pipeline"
                (default) writes N single-key GET commands in one batch —
                cluster-safe and fair to co-tenant clients. "mget" issues one
                multi-key MGET per tile — single node/proxy only.
            exists_batch_mode: how a batched-EXISTS tile is executed.
                "pipeline" (default) writes N single-key EXISTS commands in
                one batch — per-key results in one round trip, cluster-safe.
                "multikey" issues one multi-key EXISTS per tile with a
                pipelined per-key fallback for partial hits — single
                node/proxy only.

        Raises:
            RuntimeError: if the C++ Redis extension is not built, if
                get_min_keys_per_tile is < 1, if get_batch_mode is not
                "pipeline" or "mget", or if exists_batch_mode is not
                "pipeline" or "multikey".
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
        )
        super().__init__(native_client, loop)
