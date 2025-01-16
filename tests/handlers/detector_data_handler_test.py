# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import numpy as np

from beamlime.core.handler import Message, MessageKey
from beamlime.handlers.accumulators import DetectorEvents
from beamlime.handlers.detector_data_handler import DetectorHandlerFactory


def test_factory_or_mantle_detector() -> None:
    factory = DetectorHandlerFactory(instrument='dream', nexus_file='/a/b/c', config={})
    handler = factory.make_handler(
        MessageKey(topic='detector_data', source_name='mantle_detector')
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
    assert len(results) == 2  # Currently there are two mantle views configured
