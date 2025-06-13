# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from ..core.message import MessageSource
from .data_forwarder import DataForwarder


class Orchestrator:
    def __init__(
        self,
        message_source: MessageSource,
        forwarder: DataForwarder,
    ) -> None:
        self._message_source = message_source
        self._forwarder = forwarder

    def update(self) -> None:
        """
        Call this periodically to consume data and feed it into the dashboard.
        """
        messages = self._message_source.get_messages()

        if not messages:
            return

        # Batch all updates in a transaction to avoid repeated UI updates. Reason:
        # - Some listeners depend on multiple streams.
        # - There may be multiple messages for the same stream, only the last one
        #   should trigger an update.
        with self._forwarder.transaction():
            for message in messages:
                self._forwarder.forward(
                    stream_name=message.stream.name, value=message.value
                )
