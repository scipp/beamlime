# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from ..core.message import MessageSource
from .data_service import DataForwarder


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

        # The UI has listeners on all data services. Since we are processing multiple
        # messages that could all affect the same listener, we want to avoid repeated
        # UI updates. Instead, we will batch the updates and send them all at once using
        # a transaction mechanism.

        # Start transaction across all services
        self._forwarder.start_transaction()

        # Process all messages
        for message in messages:
            self._forwarder.forward(stream_name=message.stream_name, value=message.data)

        # Commit all transactions (this triggers UI updates)
        self._forwarder.commit_transaction()
