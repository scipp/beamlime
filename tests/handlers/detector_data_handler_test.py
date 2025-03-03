# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import numpy as np
import pytest
import scipp as sc

from beamlime.config.raw_detectors import available_instruments, get_config
from beamlime.core.handler import Message, MessageKey
from beamlime.handlers.accumulators import DetectorEvents
from beamlime.handlers.detector_data_handler import (
    DetectorHandlerFactory,
    get_nexus_geometry_filename,
)


def test_factory_can_fall_back_to_configured_detector_number_for_LogicalView() -> None:
    factory = DetectorHandlerFactory(instrument='dummy', config={})
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
    results = handler.handle([message])
    assert len(results) == 2  # Detector view and ROI-based TOA histogram
    counts = results[0].value.sum()['current'].data
    assert sc.identical(counts, sc.scalar(3, unit='counts', dtype='int32'))


@pytest.mark.parametrize('instrument', ['dream', 'loki'])
def test_get_nexus_filename_returns_file_for_given_date(instrument: str) -> None:
    filename = get_nexus_geometry_filename(
        instrument, date=sc.datetime('2025-01-02T00:00:00')
    )
    assert str(filename).endswith(f'geometry-{instrument}-2025-01-01.nxs')


def test_get_nexus_filename_uses_current_date_by_default() -> None:
    auto = get_nexus_geometry_filename('dream')
    explicit = get_nexus_geometry_filename('dream', date=sc.datetime('now'))
    assert auto == explicit


def test_get_nexus_filename_raises_if_instrument_unknown() -> None:
    with pytest.raises(ValueError, match='No geometry files found for instrument'):
        get_nexus_geometry_filename('abcde', date=sc.datetime('2025-01-01T00:00:00'))


def test_get_nexus_filename_raises_if_datetime_out_of_range() -> None:
    with pytest.raises(ValueError, match='No geometry file found for given date'):
        get_nexus_geometry_filename('dream', date=sc.datetime('2020-01-01T00:00:00'))


@pytest.mark.parametrize('instrument', available_instruments())
def test_factory_can_create_handler(instrument: str) -> None:
    config = get_config(instrument).detectors_config
    detectors = {view['detector_name'] for view in config['detectors'].values()}
    assert len(detectors) > 0
    factory = DetectorHandlerFactory(instrument=instrument, config={})
    for name in detectors:
        _ = factory.make_handler(MessageKey(topic='ignored', source_name=name))
