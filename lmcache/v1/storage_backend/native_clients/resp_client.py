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
        mget_min_keys_per_tile: int = 8,
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
            mget_min_keys_per_tile: minimum keys a batched GET tile carries
                before the batch is split across more worker connections
                (must be >= 1). Higher values favor fewer, larger MGET
                commands; lower values favor connection-level parallelism.

        Raises:
            RuntimeError: if the C++ Redis extension is not built, or if
                mget_min_keys_per_tile is < 1.
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
            mget_min_keys_per_tile,
        )
        super().__init__(native_client, loop)
