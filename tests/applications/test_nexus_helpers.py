# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os

from beamlime.applications._nexus_helpers import NexusContainer
from beamlime.applications._random_data_providers import (
    DetectorNumberCands,
    random_ev44_generator,
)


def test_nexus_container_initialized_from_path():
    path = os.path.join(os.path.dirname(__file__), 'ymir.json')
    NexusContainer.from_template_file(path)


def test_event_generator():
    generator = random_ev44_generator(
        detector_numbers=DetectorNumberCands(list(range(100)))
    )
    events = next(generator)
    assert 99_900 < len(events) < 10_100
    assert len(list(events['reference_time'])) == len(list(events['time_of_flight']))
    assert len(list(events['reference_time'])) == len(list(events['pixel_id']))
