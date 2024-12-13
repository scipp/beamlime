# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import scipp as sc

from beamlime.v2.core.message import Message, MessageSink


class PlotToPngSink(MessageSink[sc.DataArray]):
    def publish_messages(self, messages: Message[sc.DataArray]) -> None:
        for msg in messages:
            title = f"{msg.key.topic} - {msg.key.source_name}"
            filename = f"{msg.key.topic}_{msg.key.source_name}.png"
            msg.value.plot(title=title).save(filename)
