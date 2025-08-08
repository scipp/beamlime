# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from .message import Message


class BatchState(Enum):
    INCOMPLETE = "incomplete"
    COMPLETE = "complete"
    LATE = "late"


@dataclass(slots=True, kw_only=True)
class MessageBatch:
    start_time: int
    end_time: int
    state: BatchState
    messages: list[Message[Any]]


class MessageBatcher:
    """
    Batches messages based on a fixed time interval.

    This batcher collects messages into batches of a fixed length, defined by
    `batch_length_s`. The start_time and end_time are the start end end of the time
    interval belonging to the batch, even if there is no message at those times.

    All but the first batch are marked as ready, meaning that they can be processed
    immediately. Late messages are marked as `BatchState.LATE`, allowing the caller to
    decide how to handle them.
    """

    def __init__(self, batch_length_s: float = 1.0) -> None:
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)
        self._highest_completed_batch_id: int = -1

    def batch(self, messages: list[Message[Any]]) -> list[MessageBatch]:
        if not messages:
            return []

        # Group messages by batch intervals
        batch_groups: dict[int, list[Message[Any]]] = {}
        for msg in messages:
            batch_id = msg.timestamp // self._batch_length_ns
            if batch_id not in batch_groups:
                batch_groups[batch_id] = []
            batch_groups[batch_id].append(msg)

        # Sort batch IDs to process in chronological order
        sorted_batch_ids = sorted(batch_groups.keys())

        result: list[MessageBatch] = []
        for i, batch_id in enumerate(sorted_batch_ids):
            start_time = batch_id * self._batch_length_ns
            end_time = start_time + self._batch_length_ns

            # Determine batch state. In the future we may consider a more sophisticated
            # logic, e.g., by tracking if we have seen detector events for all streams,
            # as well as tracking whether there are ever multiple detector event
            # messages for a single pulse and how often the second message arrives late
            # or out of order.
            if batch_id <= self._highest_completed_batch_id:
                state = BatchState.LATE
            elif i < len(sorted_batch_ids) - 1:
                # All but the last (most recent) batch are complete
                state = BatchState.COMPLETE
                self._highest_completed_batch_id = batch_id
            else:
                # The most recent batch is incomplete
                state = BatchState.INCOMPLETE

            batch = MessageBatch(
                start_time=start_time,
                end_time=end_time,
                state=state,
                messages=batch_groups[batch_id],
            )
            result.append(batch)

        return result
