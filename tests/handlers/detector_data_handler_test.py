# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import numpy as np
import pytest
import scipp as sc

from beamlime.core.handler import Message, MessageKey
from beamlime.handlers.accumulators import DetectorEvents
from beamlime.handlers.detector_data_handler import (
    DetectorHandlerFactory,
    get_nexus_geometry_filename,
)


def test_factory_can_fall_back_to_configured_detector_number_for_LogicalView() -> None:
    factory = DetectorHandlerFactory(instrument='dummy', nexus_file=None, config={})
    handler = factory.make_handler(
        MessageKey(topic='detector_data', source_name='panel_0')
    )
    events = DetectorEvents(
        pixel_id=np.array([1, 2, 3]),
        time_of_arrival=np.array([4, 5, 6]),
        unit='ns',
    )
    message = Message(
        timestamp=1234,
        key=MessageKey(topic='abcde', source_name='ignored'),
        value=events,
    )
    results = handler.handle(message)
    assert len(results) == 1
    counts = results[0].value.sum().data
    assert sc.identical(counts, sc.scalar(3, unit='counts', dtype='int32'))


def test_get_nexus_filename_returns_file_for_given_date() -> None:
    filename = get_nexus_geometry_filename(
        'dream', date=sc.datetime('2025-01-02T00:00:00')
    )
    assert str(filename).endswith('geometry-dream-2025-01-01.nxs')


def test_get_nexus_filename_uses_current_date_by_default() -> None:
    auto = get_nexus_geometry_filename('dream')
    explicit = get_nexus_geometry_filename('dream', date=sc.datetime('now'))
    assert auto == explicit


def test_get_nexus_filename_raises_if_datetime_out_of_range() -> None:
    with pytest.raises(IndexError):
        get_nexus_geometry_filename('dream', date=sc.datetime('2020-01-01T00:00:00'))
