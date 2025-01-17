# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import numpy as np
import pytest
import scipp as sc

from beamlime.core.handler import Message, MessageKey
from beamlime.handlers.accumulators import DetectorEvents
from beamlime.handlers.detector_data_handler import DetectorHandlerFactory


def test_factory_raises_if_no_nexus_file_and_failed_fallback() -> None:
    factory = DetectorHandlerFactory(instrument='dream', nexus_file=None, config={})
    with pytest.raises(ValueError, match='NeXus file is required'):
        factory.make_handler(
            MessageKey(topic='detector_data', source_name='mantle_detector')
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
