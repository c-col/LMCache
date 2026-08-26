# SPDX-License-Identifier: Apache-2.0

"""Connector stage-timing metrics subscriber.

Consumes ``L2_CONNECTOR_BATCH_TIMING`` events (published by the native
connector L2 adapter's demux thread from the native client's
``drain_batch_timings()``) and decomposes each batch's end-to-end latency
into stages, emitted as OTel histograms in seconds, labeled by ``op``
(get/set/exists/delete) and ``backend`` (the registered adapter type,
e.g. ``"resp"``):

  - ``lmcache_mp.l2_connector_queue_time``    — submit -> first tile dequeued
    (time spent waiting for a connector worker thread)
  - ``lmcache_mp.l2_connector_dispatch_time`` — first dequeue -> first reply
    byte (command send + backend service time; skipped when the backend
    never reported a first byte, e.g. SET/EXISTS/DELETE)
  - ``lmcache_mp.l2_connector_transfer_time`` — first byte (or dequeue when
    absent) -> last tile done (payload transfer across all tiles)
  - ``lmcache_mp.l2_connector_handoff_time``  — last tile done -> drained by
    the Python demux thread (eventfd wakeup + drain latency)
  - ``lmcache_mp.l2_connector_total_time``    — submit -> drained

plus two counters:

  - ``lmcache_mp.l2_connector_transferred`` (bytes) — real bytes moved per
    batch (sum of buffer lengths; 0 for exists/delete)
  - ``lmcache_mp.l2_connector_batches`` — number of batches observed

No correlation state is needed: each event is a self-describing record.

Clock notes: the four ``t_submit``/``t_first_dequeue``/``t_first_byte``/
``t_last_done`` stamps come from the connector's monotonic-anchored wall
clock (csrc/storage_backends/connector_clock.h) and cannot go backward
relative to each other.  ``t_consumed`` is Python ``time.time()``, which NTP
can slew, so the handoff stage is clamped at 0 instead of dropped.
"""

# Future
from __future__ import annotations

# Standard
from typing import Any

# Third Party
from opentelemetry import metrics

# First Party
from lmcache.logging import init_logger
from lmcache.v1.mp_observability.event import Event, EventType
from lmcache.v1.mp_observability.event_bus import EventCallback, EventSubscriber

logger = init_logger(__name__)


class L2ConnectorStageMetricsSubscriber(EventSubscriber):
    """Records per-batch connector stage latencies from timing events."""

    def __init__(self) -> None:
        meter = metrics.get_meter("lmcache_mp.perf")
        self._queue_hist = meter.create_histogram(
            "lmcache_mp.l2_connector_queue_time",
            description=(
                "Histogram of connector batch queue time in seconds: batch "
                "submitted -> first tile dequeued by a worker thread."
            ),
            unit="s",
        )
        self._dispatch_hist = meter.create_histogram(
            "lmcache_mp.l2_connector_dispatch_time",
            description=(
                "Histogram of connector batch dispatch time in seconds: "
                "first tile dequeued -> first reply byte received.  Only "
                "recorded for ops whose backend reports a first byte "
                "(GET on the RESP connector)."
            ),
            unit="s",
        )
        self._transfer_hist = meter.create_histogram(
            "lmcache_mp.l2_connector_transfer_time",
            description=(
                "Histogram of connector batch transfer time in seconds: "
                "first reply byte (or first dequeue when the backend "
                "reports no first byte) -> last tile finished."
            ),
            unit="s",
        )
        self._handoff_hist = meter.create_histogram(
            "lmcache_mp.l2_connector_handoff_time",
            description=(
                "Histogram of connector batch handoff time in seconds: "
                "last tile finished -> timing record drained by the Python "
                "demux thread (eventfd wakeup + drain latency).  Clamped "
                "at 0 (t_consumed comes from a different clock)."
            ),
            unit="s",
        )
        self._total_hist = meter.create_histogram(
            "lmcache_mp.l2_connector_total_time",
            description=(
                "Histogram of connector batch total time in seconds: batch "
                "submitted -> drained by the Python demux thread."
            ),
            unit="s",
        )
        self._bytes_counter = meter.create_counter(
            "lmcache_mp.l2_connector_transferred",
            description=(
                "Total bytes moved through the native connector, summed "
                "over batches (buffer bytes for get/set; 0 for "
                "exists/delete)."
            ),
            unit="By",
        )
        self._batches_counter = meter.create_counter(
            "lmcache_mp.l2_connector_batches",
            description="Number of connector batches observed.",
            unit="batches",
        )

    # -- EventSubscriber interface -----------------------------------------

    def get_subscriptions(self) -> dict[EventType, EventCallback]:
        return {
            EventType.L2_CONNECTOR_BATCH_TIMING: self._on_batch_timing,
        }

    # -- Recording -----------------------------------------------------------

    def _on_batch_timing(self, event: Event) -> None:
        md = event.metadata
        try:
            t_submit = float(md["t_submit"])
            t_dequeue = float(md["t_first_dequeue"])
            t_first_byte = float(md["t_first_byte"])
            t_last_done = float(md["t_last_done"])
            t_consumed = float(md["t_consumed"])
            total_bytes = int(md.get("total_bytes", 0))
        except (KeyError, TypeError, ValueError):
            logger.debug("Malformed connector batch timing event: %s", md)
            return

        attrs: dict[str, Any] = {}
        op = md.get("op")
        if op is not None:
            attrs["op"] = str(op)
        backend = md.get("backend")
        if backend is not None:
            attrs["backend"] = str(backend)

        # The C++ stamps share one monotonic-anchored clock; a negative
        # stage means a malformed record, not clock skew — drop it.
        queue_s = t_dequeue - t_submit
        transfer_start = t_first_byte if t_first_byte > 0.0 else t_dequeue
        transfer_s = t_last_done - transfer_start
        if queue_s < 0 or transfer_s < 0:
            logger.debug(
                "Dropping connector batch timing with negative stage: %s", md
            )
            return

        self._queue_hist.record(queue_s, attributes=attrs)
        if t_first_byte > 0.0:
            dispatch_s = t_first_byte - t_dequeue
            if dispatch_s >= 0:
                self._dispatch_hist.record(dispatch_s, attributes=attrs)
        self._transfer_hist.record(transfer_s, attributes=attrs)
        # t_consumed is Python time.time() (slewable) against the anchored
        # C++ clock: clamp instead of drop.
        self._handoff_hist.record(max(0.0, t_consumed - t_last_done), attributes=attrs)
        self._total_hist.record(max(0.0, t_consumed - t_submit), attributes=attrs)
        if total_bytes > 0:
            self._bytes_counter.add(total_bytes, attributes=attrs)
        self._batches_counter.add(1, attributes=attrs)
