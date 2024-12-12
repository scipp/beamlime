# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import replace

import numpy as np
import scipp as sc
from scipp.testing import assert_identical

from beamlime.v2.core.handler import Message, MessageKey
from beamlime.v2.handlers.monitor_data_handler import (
    Histogrammer,
    MonitorDataHandler,
    MonitorEvents,
)


def test_histogrammer_returns_zeros_if_no_chunks_added() -> None:
    histogrammer = Histogrammer(config={'time_of_arrival_bins': 7})
    da = histogrammer.histogram()
    dim = 'time_of_arrival'
    bins = sc.linspace(dim, 0.0, 1000 / 14, num=7, unit='ms')
    assert_identical(
        da,
        sc.DataArray(
            sc.zeros(dims=[dim], shape=[6], unit='counts', dtype='int64'),
            coords={dim: bins},
        ),
    )


def test_handler() -> None:
    handler = MonitorDataHandler(config={'sliding_window_seconds': 10})
    msg = Message(
        timestamp=0,
        key=MessageKey(topic='monitors', source_name='monitor1'),
        value=MonitorEvents(time_of_arrival=np.array([int(1e6), int(2e6), int(4e7)])),
    )
    results = handler.handle(msg)
    assert len(results) == 2
    assert sc.identical(results[0].value, results[1].value)

    results = handler.handle(msg)
    # No update since we are still in same update interval
    assert len(results) == 0

    msg = replace(msg, timestamp=msg.timestamp + int(1.2e9))
    results = handler.handle(msg)
    assert len(results) == 2
    # Everything is still in same window
    assert sc.identical(results[0].value, results[1].value)

    msg = replace(msg, timestamp=msg.timestamp + int(9e9))
    results = handler.handle(msg)
    assert len(results) == 2
    # Window contains 3 messages (should be 2, but due to initial update on first
    # message, we have 3), 4 messages since start.
    assert_identical(4 * results[0].value, 3 * results[1].value)
