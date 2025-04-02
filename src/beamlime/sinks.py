# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging

import scipp as sc

from .core import Message, MessageSink, compact_messages


class PlotToPngSink(MessageSink[sc.DataArray]):
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def publish_messages(self, messages: Message[sc.DataArray]) -> None:
        for msg in compact_messages(messages):
            title = f"{msg.key.kind} - {msg.key.name}"
            # Normalize the source name to be a valid filename and not a directory
            filename = f"{msg.key.kind}_{msg.key.name.replace('/', '_')}.png"
            try:
                msg.value.plot(title=title).save(filename)
            except Exception:
                self.logger.exception("Plotting to PNG failed")
                pass
