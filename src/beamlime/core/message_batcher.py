# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .message import Message


@dataclass
class MessageBatch:
    start_time: int
    end_time: int
    ready: bool
    messages: list[Message[Any]]


class MessageBatcher:
    """
    Batches messages based on their timestamps.

    Message timestamps are typically given be the neutron source pulse timestamp, which
    is approximately occuring at regular intervals, e.g., 14 Hz. This batcher collects
    messages from N (configurable) pulses into batches. The start_time of batch B is the
    end_time of batch B-1.

    The batcher determines whether a batch is ready using a history of batch calls and
    a dynamic backoff mechanism. For example, in 0th approximation we expect that each
    detector event stream will produce one message per pulse. If we have seen such a
    message for each detector stream we can consider the batch ready. If we observe
    late messages (in the next call to `batch`), we will increase the backoff time
    and wait for more messages before considering the batch ready.
    """

    def __init__(self, batch_length_s: float = 1.0) -> None:
        self._batch_length_ns = int(batch_length_s * 1_000_000_000)

    def batch(self, messages: list[Message[Any]]) -> list[MessageBatch]:
        if not messages:
            return []
        timestamps = np.array([msg.timestamp for msg in messages])
        batches = timestamps // self._batch_length_ns
        result: dict[int, MessageBatch] = {}
        # track what was marked as ready already? include in next batch??
        # or just return that batch again? hopefully rarely causing issues?
        # No! accum assumes we do this in order, since it has clearing logic.
        # ready marking must be increasing, with no gaps.
