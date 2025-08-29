# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from numbers import Number
from typing import Any

from beamlime.core.message import Message


@dataclass(slots=True, kw_only=True)
class MessageBatch:
    start_time: int
    end_time: int
    messages: list[Message[Any]]


class NaiveMessageBatcher:
    def __init__(
        self, batch_length_s: float = 1.0, pulse_length_s: float = 1.0 / 14
    ) -> None:
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)
        self._pulse_length_ns = int(pulse_length_s * 1_000_000_000)
        self._watermark: int | None = None
        self._buffered_messages: list[Message[Any]] = []

    def batch(self, messages: list[Message[Any]]) -> MessageBatch | None:
        # Unclear if filter needed in practice, but there is a test with bad timestamps.
        messages = [msg for msg in messages if isinstance(msg.timestamp, Number)]
        if not messages:
            # Return any buffered messages if we have them and a watermark is set
            if self._buffered_messages and self._watermark is not None:
                return self._create_batch_from_buffered()
            return None

        # Add new messages to buffer
        self._buffered_messages.extend(messages)
        self._buffered_messages = sorted(self._buffered_messages)

        # Initialize watermark on first call
        max_timestamp = max(msg.timestamp for msg in self._buffered_messages)
        if self._watermark is None:
            self._watermark = max_timestamp
            return None  # Wait for more messages to potentially advance watermark

        # Check if we should advance watermark
        next_watermark = self._watermark + self._batch_length_ns
        if max_timestamp > next_watermark:
            self._watermark = next_watermark

        return self._create_batch_from_buffered()

    def _create_batch_from_buffered(self) -> MessageBatch | None:
        """Create a batch from buffered messages before the watermark."""
        if not self._buffered_messages or self._watermark is None:
            return None

        # Split messages: before watermark vs after watermark
        messages_before_watermark = []
        messages_after_watermark = []

        for msg in self._buffered_messages:
            if msg.timestamp <= self._watermark:
                messages_before_watermark.append(msg)
            else:
                messages_after_watermark.append(msg)

        # Keep messages after watermark for next batch
        self._buffered_messages = messages_after_watermark

        if not messages_before_watermark:
            return None

        # Use watermark directly as end_time
        end_time = self._watermark
        start_time = end_time - self._batch_length_ns

        return MessageBatch(
            start_time=start_time, end_time=end_time, messages=messages_before_watermark
        )
