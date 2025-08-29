# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from abc import ABC, abstractmethod
from dataclasses import dataclass
from numbers import Number
from typing import Any

from beamlime.core.message import Message


@dataclass(slots=True, kw_only=True)
class MessageBatch:
    start_time: int
    end_time: int
    messages: list[Message[Any]]


class MessageBatcher(ABC):
    @abstractmethod
    def batch(self, messages: list[Message[Any]]) -> MessageBatch | None:
        """Create and return a message batch if possible.

        If no batch can be created (batch incomplete), return None.
        """


class NaiveMessageBatcher(MessageBatcher):
    def __init__(
        self, batch_length_s: float = 1.0, pulse_length_s: float = 1.0 / 14
    ) -> None:
        # Batch length is currently ignored.
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)
        self._pulse_length_ns = int(pulse_length_s * 1_000_000_000)

    def batch(self, messages: list[Message[Any]]) -> MessageBatch | None:
        # Filter messages with incompatible (broken) timestamps to avoid issues below.
        messages = [msg for msg in messages if isinstance(msg.timestamp, Number)]
        if not messages:
            return None
        messages = sorted(messages)
        # start_time is the lower bound of the batch, end_time is the upper bound, both
        # in multiples of the pulse length.
        start_time = (
            messages[0].timestamp // self._pulse_length_ns * self._pulse_length_ns
        )
        end_time = (
            (messages[-1].timestamp + self._pulse_length_ns - 1)
            // self._pulse_length_ns
            * self._pulse_length_ns
        )
        return MessageBatch(start_time=start_time, end_time=end_time, messages=messages)


class SimpleMessageBatcher(MessageBatcher):
    def __init__(self, batch_length_s: float = 1.0) -> None:
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)
        self._active_batch: MessageBatch | None = None
        self._future_messages: list[Message[Any]] = []

    def batch(self, messages: list[Message[Any]]) -> MessageBatch | None:
        # Filter messages with incompatible (broken) timestamps to avoid issues below.
        messages = [msg for msg in messages if isinstance(msg.timestamp, Number)]

        # Create and return initial batch including everything
        if self._active_batch is None:
            return self._make_initial_batch(messages)

        # We have an active batch, decide which messages belong to it
        new_active, after = self._split_messages(messages, self._active_batch.end_time)
        self._active_batch.messages.extend(new_active)
        self._future_messages.extend(after)

        # No future messages, assume we will get more messages for the active batch.
        # Note this is different from returning an empty batch.
        if not self._future_messages:
            return None

        # We have future messages, i.e., we assume the active batch is done. This may
        # return an empty batch, which is desired behavior, i.e., we advance batch by
        # batch.
        batch = self._active_batch
        new_end_time = batch.end_time + self._batch_length_ns
        new_active, self._future_messages = self._split_messages(
            self._future_messages, new_end_time
        )
        self._active_batch = MessageBatch(
            start_time=batch.end_time, end_time=new_end_time, messages=new_active
        )
        return batch

    def _make_initial_batch(self, messages: list[Message[Any]]) -> MessageBatch | None:
        """Make initial batch that includes everything."""
        if not messages:
            return None
        start_time = min(msg.timestamp for msg in messages)
        end_time = max(msg.timestamp for msg in messages)
        batch = MessageBatch(
            start_time=start_time, end_time=end_time, messages=messages
        )
        # After the initial batch we align to batch boundaries.  The next batch starts
        # immediately after the initial batch ends.
        next_start = end_time
        self._active_batch = MessageBatch(
            start_time=next_start,
            end_time=next_start + self._batch_length_ns,
            messages=[],
        )
        return batch

    def _split_messages(
        self, messages: list[Message[Any]], timestamp: int
    ) -> tuple[list[Message[Any]], list[Message[Any]]]:
        # Note that the batch start time will "lie" if there are late messages, but this
        # is currently considered acceptable and better than dropping messages.
        before = [msg for msg in messages if msg.timestamp < timestamp]
        after = [msg for msg in messages if msg.timestamp >= timestamp]
        return before, after
