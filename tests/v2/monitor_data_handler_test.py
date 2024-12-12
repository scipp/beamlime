# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import replace

import numpy as np
import pytest
import scipp as sc
from scipp.testing import assert_identical

from beamlime.v2.core.handler import Message, MessageKey
from beamlime.v2.handlers.monitor_data_handler import (
    Histogrammer,
    MonitorDataHandler,
    MonitorEvents,
)


def test_histogrammer_returns_zeros_if_no_chunks_added() -> None:
    histogrammer = Histogrammer()
    bins = sc.linspace('x', 0.0, 10.0, num=7, unit='ms')
    da = histogrammer.histogram(bins)
    assert_identical(
        da,
        sc.DataArray(
            sc.zeros(dims=['x'], shape=[6], unit='counts', dtype='int64'),
            coords={'x': bins},
        ),
    )


@pytest.mark.parametrize('dim', ['x', 'toa'])
@pytest.mark.parametrize('unit', ['ms', 's'])
def test_histogrammer_uses_dim_and_unit_of_bins(dim: str, unit: str) -> None:
    histogrammer = Histogrammer()
    histogrammer.add(np.array([1.0, 2.0, 3.0]))
    bins = sc.linspace(dim, 0.0, 10.0, num=7, unit=unit)
    da = histogrammer.histogram(bins)
    assert da.coords[dim].unit == unit
    assert da.dim == dim
    assert da.unit == 'counts'


def test_handler() -> None:
    handler = MonitorDataHandler(config={})
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
