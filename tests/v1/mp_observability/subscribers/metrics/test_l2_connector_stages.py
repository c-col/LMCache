# SPDX-License-Identifier: Apache-2.0

"""Tests for L2ConnectorStageMetricsSubscriber.

Uses ``InMemoryMetricReader`` (via ``otel_setup``) to read back OTel
histogram values.  Handlers are invoked directly for determinism — the
event-bus wiring is exercised by the adapter tests that publish
``L2_CONNECTOR_BATCH_TIMING``.
"""

# Third Party
import pytest

# First Party
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.subscribers.metrics.l2_connector_stages import (
    L2ConnectorStageMetricsSubscriber,
)
from tests.v1.mp_observability.subscribers.metrics.otel_setup import (
    reader as _reader,
)

_QUEUE = "lmcache_mp.l2_connector_queue_time"
_DISPATCH = "lmcache_mp.l2_connector_dispatch_time"
_TRANSFER = "lmcache_mp.l2_connector_transfer_time"
_HANDOFF = "lmcache_mp.l2_connector_handoff_time"
_TOTAL = "lmcache_mp.l2_connector_total_time"
_BYTES = "lmcache_mp.l2_connector_transferred"
_BATCHES = "lmcache_mp.l2_connector_batches"

_T0 = 1_700_000_000.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _timing_event(
    op: str = "get",
    backend: str = "resp",
    total_bytes: int = 1_000_000,
    t_submit: float = _T0,
    t_first_dequeue: float = _T0 + 0.010,
    t_first_byte: float = _T0 + 0.015,
    t_last_done: float = _T0 + 0.115,
    t_consumed: float = _T0 + 0.120,
) -> Event:
    return Event(
        event_type=EventType.L2_CONNECTOR_BATCH_TIMING,
        metadata={
            "op": op,
            "num_keys": 8,
            "total_bytes": total_bytes,
            "t_submit": t_submit,
            "t_first_dequeue": t_first_dequeue,
            "t_first_byte": t_first_byte,
            "t_last_done": t_last_done,
            "t_consumed": t_consumed,
            "backend": backend,
        },
    )


def _read_metrics() -> dict[str, list]:
    data = _reader.get_metrics_data()
    result: dict[str, list] = {}
    if data is None:
        return result
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                result.setdefault(metric.name, []).extend(metric.data.data_points)
    return result


def _hist_count(name: str) -> int:
    return sum(dp.count for dp in _read_metrics().get(name, []))


def _hist_sum(name: str) -> float:
    return sum(dp.sum for dp in _read_metrics().get(name, []))


def _counter_total(name: str) -> int:
    return sum(int(dp.value) for dp in _read_metrics().get(name, []))


def _snapshot() -> dict[str, float]:
    """Snapshot counts/sums for all our metrics (the reader is shared
    across test files, so every assertion works on deltas)."""
    return {
        f"{m}.count": _hist_count(m)
        for m in (_QUEUE, _DISPATCH, _TRANSFER, _HANDOFF, _TOTAL)
    } | {
        f"{m}.sum": _hist_sum(m)
        for m in (_QUEUE, _DISPATCH, _TRANSFER, _HANDOFF, _TOTAL)
    } | {
        _BYTES: _counter_total(_BYTES),
        _BATCHES: _counter_total(_BATCHES),
    }


def _delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    return {k: after[k] - before[k] for k in after}


@pytest.fixture
def subscriber():
    return L2ConnectorStageMetricsSubscriber()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_subscription_map(subscriber):
    subs = subscriber.get_subscriptions()
    assert set(subs) == {EventType.L2_CONNECTOR_BATCH_TIMING}


def test_stage_values_in_seconds(subscriber):
    before = _snapshot()
    subscriber._on_batch_timing(_timing_event())
    d = _delta(before, _snapshot())

    assert d[f"{_QUEUE}.count"] == 1
    assert d[f"{_QUEUE}.sum"] == pytest.approx(0.010, abs=1e-6)
    assert d[f"{_DISPATCH}.count"] == 1
    assert d[f"{_DISPATCH}.sum"] == pytest.approx(0.005, abs=1e-6)
    assert d[f"{_TRANSFER}.count"] == 1
    assert d[f"{_TRANSFER}.sum"] == pytest.approx(0.100, abs=1e-6)
    assert d[f"{_HANDOFF}.count"] == 1
    assert d[f"{_HANDOFF}.sum"] == pytest.approx(0.005, abs=1e-6)
    assert d[f"{_TOTAL}.count"] == 1
    assert d[f"{_TOTAL}.sum"] == pytest.approx(0.120, abs=1e-6)
    assert d[_BYTES] == 1_000_000
    assert d[_BATCHES] == 1


def test_missing_first_byte_skips_dispatch(subscriber):
    """t_first_byte == 0.0 (e.g. SET): no dispatch sample; transfer is
    measured from first dequeue instead."""
    before = _snapshot()
    subscriber._on_batch_timing(_timing_event(op="set", t_first_byte=0.0))
    d = _delta(before, _snapshot())

    assert d[f"{_DISPATCH}.count"] == 0
    assert d[f"{_TRANSFER}.count"] == 1
    # transfer = t_last_done - t_first_dequeue = 0.105
    assert d[f"{_TRANSFER}.sum"] == pytest.approx(0.105, abs=1e-6)
    assert d[f"{_TOTAL}.count"] == 1


def test_negative_handoff_clamped(subscriber):
    """t_consumed comes from a different (slewable) clock: clamp, not drop."""
    before = _snapshot()
    subscriber._on_batch_timing(
        _timing_event(t_consumed=_T0 + 0.114)  # before t_last_done
    )
    d = _delta(before, _snapshot())

    assert d[f"{_HANDOFF}.count"] == 1
    assert d[f"{_HANDOFF}.sum"] == pytest.approx(0.0, abs=1e-9)


def test_negative_stage_dropped(subscriber):
    """Negative C++-clock stage means a malformed record: drop entirely."""
    before = _snapshot()
    subscriber._on_batch_timing(
        _timing_event(t_first_dequeue=_T0 - 1.0)  # before t_submit
    )
    d = _delta(before, _snapshot())
    assert d[_BATCHES] == 0
    assert d[f"{_QUEUE}.count"] == 0


def test_malformed_event_dropped(subscriber):
    before = _snapshot()
    subscriber._on_batch_timing(
        Event(
            event_type=EventType.L2_CONNECTOR_BATCH_TIMING,
            metadata={"op": "get"},  # missing all timestamps
        )
    )
    d = _delta(before, _snapshot())
    assert d[_BATCHES] == 0


def test_zero_bytes_skips_bytes_counter(subscriber):
    """exists/delete batches carry total_bytes == 0."""
    before = _snapshot()
    subscriber._on_batch_timing(
        _timing_event(op="exists", total_bytes=0, t_first_byte=0.0)
    )
    d = _delta(before, _snapshot())
    assert d[_BYTES] == 0
    assert d[_BATCHES] == 1


def test_attributes_carry_op_and_backend(subscriber):
    subscriber._on_batch_timing(_timing_event(op="get", backend="resp"))
    attrs = [
        dict(dp.attributes)
        for dp in _read_metrics().get(_QUEUE, [])
        if dp.count > 0
    ]
    assert {"op": "get", "backend": "resp"} in attrs
