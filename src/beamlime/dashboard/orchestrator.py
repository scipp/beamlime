# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from ..core.message import MessageSource
from .data_service import DataForwarder


class Orchestrator:
    def __init__(self, message_source: MessageSource, forwarder: DataForwarder):
        self._message_source = message_source
        self._forwarder = forwarder

    def update(self):
        """
        Call this periodically to consume data and feed it into the dashboard.
        """
        messages = self._message_source.get_messages()
        for message in messages:
            self._forwarder.forward(stream_name=message.stream_name, value=message.data)
        # TODO How to push data into panel.Pipe.send? Options:
        # - Register callbacks in data services?
        #   Each callback should react on a set of keys, extract and combine data.
        # - How can we avoid triggering multiple times?
        # - Should we pull instead, checking for updates? Would need to keep track of
        #   the keys that have been updated.
        # - Or do it transaction based? Have the orchestrator start a transaction in
        #   each data service, then push all updates, and finally commit the
        #   transaction?
