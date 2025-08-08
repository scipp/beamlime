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

    If there are messages this always returns either a single batch (INCOMPLETE) or two
    batches (one COMPLETE and one INCOMPLETE). Internally this maintains timestamp used
    to split into batches.

    - On the first call (with messages), init the timestamp to the timestamp of the
      first message plus `batch_length_s`.
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
    """

    def __init__(self, batch_length_s: float = 1.0) -> None:
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)
        self._current_timestamp: int | None = None

    def batch(self, messages: list[Message[Any]]) -> list[MessageBatch]:
        if not messages:
            return []

        messages = sorted(messages)

        # Initialize timestamp on first call
        if self._current_timestamp is None:
            self._current_timestamp = messages[0].timestamp + self._batch_length_ns

        # Split messages based on current timestamp
        first_batch_messages = []
        second_batch_messages = []

        for message in messages:
            if message.timestamp < self._current_timestamp:
                first_batch_messages.append(message)
            else:
                second_batch_messages.append(message)

        batches = []

        # TODO start/end timestamps are wrong if we skipped multiple.
        # All this seems like a bad idea.
        if second_batch_messages:
            # First batch is COMPLETE, second batch is INCOMPLETE
            first_batch = MessageBatch(
                start_time=self._current_timestamp - self._batch_length_ns,
                end_time=self._current_timestamp,
                state=BatchState.COMPLETE,
                messages=first_batch_messages,
            )
            batches.append(first_batch)

            # Update timestamp to next interval after last message
            last_message_time = second_batch_messages[-1].timestamp
            self._current_timestamp = (
                (last_message_time // self._batch_length_ns) + 1
            ) * self._batch_length_ns

            incomplete_messages = second_batch_messages
        else:
            # Only one INCOMPLETE batch, timestamp not incremented
            incomplete_messages = first_batch_messages

        second_batch = MessageBatch(
            start_time=self._current_timestamp - self._batch_length_ns,
            end_time=self._current_timestamp,
            state=BatchState.INCOMPLETE,
            messages=incomplete_messages,
        )
        batches.append(second_batch)

        return batches
