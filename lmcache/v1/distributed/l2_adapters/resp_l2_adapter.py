# SPDX-License-Identifier: Apache-2.0
"""
RESP (Redis/Valkey) L2 adapter config and factory.

Backed by the native C++ Redis connector wrapped with
``NativeConnectorL2Adapter``.
"""

# Future
from __future__ import annotations

# Standard
from typing import TYPE_CHECKING, Optional
import os

if TYPE_CHECKING:
    from lmcache.v1.distributed.internal_api import (
        L1MemoryDesc,
    )

# First Party
from lmcache.logging import init_logger
from lmcache.v1.distributed.l2_adapters.base import (
    L2AdapterInterface,
)
from lmcache.v1.distributed.l2_adapters.config import (
    L2AdapterConfigBase,
    register_l2_adapter_type,
)
from lmcache.v1.distributed.l2_adapters.factory import (
    register_l2_adapter_factory,
)

logger = init_logger(__name__)


class RESPL2AdapterConfig(L2AdapterConfigBase):
    """
    Config for an L2 adapter backed by a native RESP
    connector (Redis/Valkey).

    Fields:
    - host: server hostname or IP.
    - port: server port.
    - num_workers: C++ worker threads for I/O (default 8).
    - username: optional auth username.
    - password: optional auth password.
    - get_min_keys_per_tile: DEPRECATED in favor of get_target_tile_bytes.
      Minimum keys per batched-GET tile before the batch is split across
      more worker connections (default 1 = no effect, >= 1; values > 1 act
      as a secondary cap on the byte-based tile count).
    - get_batch_mode: "pipeline" (default and only supported batched mode,
      cluster-safe), "mget" (deprecated; one multi-key command per tile,
      single node/proxy only), or "single" (benchmarking baseline: one
      blocking round trip per key; a miss desyncs the connection, so only
      use against fully-hit keyspaces).
    - exists_batch_mode: "pipeline" (default, cluster-safe) or "multikey"
      (one multi-key EXISTS per tile with a pipelined per-key fallback for
      partial hits; single node/proxy only).
    - get_target_tile_bytes: target payload bytes per pipelined-GET tile;
      tile count ~= ceil(batch_bytes / target), clamped to [1, workers]
      (default 32_000_000, >= 1).
    """

    def __init__(
        self,
        host: str,
        port: int,
        num_workers: int = 8,
        username: str = "",
        password: str = "",
        max_capacity_gb: float = 0,
        get_min_keys_per_tile: int = 1,
        get_batch_mode: str = "pipeline",
        exists_batch_mode: str = "pipeline",
        get_target_tile_bytes: int = 32_000_000,
    ):
        super().__init__()
        self.host = host
        self.port = port
        self.num_workers = num_workers
        self.username = username
        self.password = password
        self.max_capacity_gb = max_capacity_gb
        self.get_min_keys_per_tile = get_min_keys_per_tile
        self.get_batch_mode = get_batch_mode
        self.exists_batch_mode = exists_batch_mode
        self.get_target_tile_bytes = get_target_tile_bytes

    @classmethod
    def from_dict(cls, d: dict) -> "RESPL2AdapterConfig":
        host = d.get("host")
        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")

        port = d.get("port")
        if not isinstance(port, int) or port <= 0:
            raise ValueError("port must be a positive integer")

        num_workers = d.get("num_workers", 8)
        if not isinstance(num_workers, int) or num_workers <= 0:
            raise ValueError("num_workers must be a positive integer")

        username = d.get("username", "")
        password = d.get("password", "")

        max_capacity_gb = d.get("max_capacity_gb", 0)
        if not isinstance(max_capacity_gb, (int, float)) or max_capacity_gb < 0:
            raise ValueError("max_capacity_gb must be a non-negative number")

        # Fail loudly on the pre-rename key so it is not silently ignored
        if "mget_min_keys_per_tile" in d:
            raise ValueError(
                "mget_min_keys_per_tile was renamed to get_min_keys_per_tile"
            )

        get_min_keys_per_tile = d.get("get_min_keys_per_tile", 1)
        if not isinstance(get_min_keys_per_tile, int) or get_min_keys_per_tile < 1:
            raise ValueError("get_min_keys_per_tile must be a positive integer")

        get_batch_mode = d.get("get_batch_mode", "pipeline")
        if get_batch_mode not in ("pipeline", "mget", "single"):
            raise ValueError("get_batch_mode must be 'pipeline', 'mget', or 'single'")

        exists_batch_mode = d.get("exists_batch_mode", "pipeline")
        if exists_batch_mode not in ("pipeline", "multikey"):
            raise ValueError("exists_batch_mode must be 'pipeline' or 'multikey'")

        get_target_tile_bytes = d.get("get_target_tile_bytes", 32_000_000)
        if not isinstance(get_target_tile_bytes, int) or get_target_tile_bytes < 1:
            raise ValueError("get_target_tile_bytes must be a positive integer")

        return cls(
            host=host,
            port=port,
            num_workers=num_workers,
            username=str(username),
            password=str(password),
            max_capacity_gb=float(max_capacity_gb),
            get_min_keys_per_tile=get_min_keys_per_tile,
            get_batch_mode=str(get_batch_mode),
            exists_batch_mode=str(exists_batch_mode),
            get_target_tile_bytes=get_target_tile_bytes,
        )

    @classmethod
    def help(cls) -> str:
        return (
            "RESP L2 adapter config fields:\n"
            "- host (str): Redis/Valkey server hostname "
            "or IP (required)\n"
            "- port (int): server port (required, >0)\n"
            "- num_workers (int): C++ worker threads "
            "for I/O (default 8, >0)\n"
            "- username (str): auth username "
            "(default empty)\n"
            "- password (str): auth password "
            "(default empty)\n"
            "- max_capacity_gb (float): max L2 capacity "
            "in GB for usage tracking / eviction "
            "(default 0 = disabled)\n"
            "- get_min_keys_per_tile (int): DEPRECATED, "
            "use get_target_tile_bytes. Minimum keys per "
            "batched-GET tile; values >1 cap the "
            "byte-based tile count "
            "(default 1 = no effect, >=1)\n"
            "- get_batch_mode (str): 'pipeline' (default "
            "and only supported batched mode, "
            "cluster-safe pipelined GETs), 'mget' "
            "(deprecated; one multi-key MGET per tile; "
            "single node/proxy only), or 'single' "
            "(benchmarking baseline: one blocking round "
            "trip per key; misses desync the connection)\n"
            "- get_target_tile_bytes (int): target "
            "payload bytes per pipelined-GET tile; tile "
            "count ~= ceil(batch_bytes / target), "
            "clamped to [1, workers] "
            "(default 32000000, >=1)\n"
            "- exists_batch_mode (str): 'pipeline' "
            "(default, cluster-safe pipelined EXISTS) or "
            "'multikey' (one multi-key EXISTS per tile "
            "with pipelined per-key fallback; single "
            "node/proxy only)\n\n"
            "Environment variable defaults (used when "
            "config value is empty, read at adapter "
            "creation, not stored in config):\n"
            "- LMCACHE_RESP_USERNAME: default username\n"
            "- LMCACHE_RESP_PASSWORD: default password\n"
            "- LMCACHE_RESP_HOST: default host\n"
            "- LMCACHE_RESP_PORT: default port"
        )


def _create_resp_l2_adapter(
    config: L2AdapterConfigBase,
    l1_memory_desc: "Optional[L1MemoryDesc]" = None,
) -> L2AdapterInterface:
    """Create a NativeConnectorL2Adapter backed by the
    C++ Redis connector."""
    try:
        # First Party
        from lmcache.lmcache_redis import (
            LMCacheRedisClient,
        )
    except ImportError as e:
        raise RuntimeError(
            "RESP L2 adapter requires the C++ Redis "
            "extension. Build with: pip install -e ."
        ) from e

    # Lazy import to avoid circular dependency
    # First Party
    from lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter import (  # noqa: E501
        NativeConnectorL2Adapter,
    )

    assert isinstance(config, RESPL2AdapterConfig)

    # Config/CLI args take precedence over environment variables,
    # which serve as defaults. This keeps secrets out of logged
    # config while allowing explicit CLI overrides.
    host = config.host or os.environ.get("LMCACHE_RESP_HOST", "")
    port = config.port if config.port else int(os.environ.get("LMCACHE_RESP_PORT", "0"))
    username = config.username or os.environ.get("LMCACHE_RESP_USERNAME", "")
    password = config.password or os.environ.get("LMCACHE_RESP_PASSWORD", "")

    native_client = LMCacheRedisClient(
        host,
        port,
        config.num_workers,
        username,
        password,
        config.get_min_keys_per_tile,
        config.get_batch_mode,
        config.exists_batch_mode,
        config.get_target_tile_bytes,
    )
    logger.info(
        "Created RESP L2 adapter: %s:%d (workers=%d, get_min_keys_per_tile=%d, "
        "get_batch_mode=%s, exists_batch_mode=%s, get_target_tile_bytes=%d)",
        host,
        port,
        config.num_workers,
        config.get_min_keys_per_tile,
        config.get_batch_mode,
        config.exists_batch_mode,
        config.get_target_tile_bytes,
    )
    return NativeConnectorL2Adapter(
        native_client, max_capacity_gb=config.max_capacity_gb
    )


# Self-register config type and adapter factory
register_l2_adapter_type("resp", RESPL2AdapterConfig)
register_l2_adapter_factory("resp", _create_resp_l2_adapter)
