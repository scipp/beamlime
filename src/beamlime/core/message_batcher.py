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


class SimpeMessageBatcher:
    """
    Batches messages based on a fixed time interval.

    This batcher collects messages into batches of a fixed length, defined by
    `batch_length_s`. The start_time and end_time are the start end end of the time
    interval belonging to the batch, even if there is no message at those times.

    If there are messages this always returns either a single batch (INCOMPLETE) or two
    batches (one COMPLETE and one INCOMPLETE). Internally this maintains timestamp used
    to split into batches.

    - Messages below the timestamp are included in the first message.
    - Messages above the timestamp are included in the second message.
    - If there are messages in the second batch:
        - The first batch is marked as COMPLETE.
        - The second batch is marked as INCOMPLETE.
        - The timestamp is incremented the next time interval after the last message.
          Typically this means incrementing by `batch_length_s`.
    - Else:
        - The first (and only) batch is marked as INCOMPLETE.
        - The timestamp is not incremented.
    - On the first call (with messages), init the timestamp to the timestamp of the
      first message plus `batch_length_s`.
    """

    def __init__(self, batch_length_s: float = 1.0) -> None:
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)

    def batch(self, messages: list[Message[Any]]) -> list[MessageBatch]:
        if not messages:
            return []


class MessageBatcher:
    """
    Batches messages based on a fixed time interval.

    This batcher collects messages into batches of a fixed length, defined by
    `batch_length_s`. The start_time and end_time are the start end end of the time
    interval belonging to the batch, even if there is no message at those times.

    All but the first batch are marked as ready, meaning that they can be processed
    immediately. Late messages are marked as `BatchState.LATE`, allowing the caller to
    decide how to handle them.

    Batch state determination: In the future we may consider a more sophisticated logic,
    e.g., by tracking if we have seen detector events for all streams, as well as
    tracking whether there are ever multiple detector event messages for a single pulse
    and how often the second message arrives late or out of order.
    """

    def __init__(self, batch_length_s: float = 1.0) -> None:
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)
        self._highest_completed_batch_id: int = -1
        self._highest_seen_batch_id: int = -1

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

        current_highest_batch_id = max(batch_groups.keys())

        # Process all batches that have messages, including potentially late ones
        min_batch_id = min(batch_groups.keys())
        start_batch_id = min(min_batch_id, self._highest_completed_batch_id + 1)
        end_batch_id = current_highest_batch_id

        # TODO Problems:
        # - bad timestamps will cause massive loops
        # - should we just discard?

        result: list[MessageBatch] = []
        for batch_id in range(start_batch_id, end_batch_id + 1):
            start_time = batch_id * self._batch_length_ns
            end_time = start_time + self._batch_length_ns

            # Get messages for this batch (empty list if no messages)
            messages_for_batch = batch_groups.get(batch_id, [])

            # Determine batch state
            if batch_id <= self._highest_completed_batch_id:
                state = BatchState.LATE
            elif batch_id < current_highest_batch_id:
                # All but the last (most recent) batch are complete
                state = BatchState.COMPLETE
            else:
                # The most recent batch is incomplete
                state = BatchState.INCOMPLETE

            batch = MessageBatch(
                start_time=start_time,
                end_time=end_time,
                state=state,
                messages=messages_for_batch,
            )
            result.append(batch)

        # Update the highest completed batch ID (all complete batches processed this round)
        if current_highest_batch_id > self._highest_completed_batch_id:
            # Mark all batches except the most recent one as completed
            self._highest_completed_batch_id = max(
                self._highest_completed_batch_id, current_highest_batch_id - 1
            )

        self._highest_seen_batch_id = current_highest_batch_id

        return result
