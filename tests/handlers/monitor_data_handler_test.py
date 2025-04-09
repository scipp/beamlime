# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import replace

import numpy as np
from scipp.testing import assert_identical

from beamlime.core.handler import FakeConfigRegistry, Message, StreamId, StreamKind
from beamlime.handlers.monitor_data_handler import (
    MonitorEvents,
    MonitorHandlerFactory,
)


def test_handler() -> None:
    factory = MonitorHandlerFactory(config_registry=FakeConfigRegistry())
    handler = factory.make_handler(
        key=StreamId(name='monitor1', kind=StreamKind.MONITOR_EVENTS)
    )
    msg = Message(
        timestamp=0,
        stream=StreamId(kind=StreamKind.MONITOR_EVENTS, name='monitor1'),
        value=MonitorEvents(
            time_of_arrival=np.array([int(1e6), int(2e6), int(4e7)]), unit='ns'
        ),
    )
    results = handler.handle([msg])
    assert len(results) == 2
    assert_identical(results[0].value, results[1].value)

    results = handler.handle([msg])
    # No update since we are still in same update interval
    assert len(results) == 0

    msg = replace(msg, timestamp=msg.timestamp + int(1.2e9))
    results = handler.handle([msg])
    assert len(results) == 2
    # Cumulative is 3 messages, current is 2
    assert_identical(2 * results[0].value, 3 * results[1].value)

    msg = replace(msg, timestamp=msg.timestamp + int(9e9))
    results = handler.handle([msg])
    assert len(results) == 2
    # Current is 1 message, 4 messages since start.
    cumulative_value = -1
    sliding_window_value = -1
    for msg in results:
        if 'cumulative' in msg.stream.name:
            cumulative_value = msg.value
        else:
            sliding_window_value = msg.value
    assert_identical(cumulative_value, 4 * sliding_window_value)
