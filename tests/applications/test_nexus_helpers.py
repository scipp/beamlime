# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os

import pytest

from beamlime.applications._nexus_helpers import NexusContainer
from beamlime.applications._random_data_providers import (
    DetectorNumberCandidates,
    RandomEV44Generator,
    random_ev44_generator,
)


def test_nexus_container_initialized_from_path():
    path = os.path.join(os.path.dirname(__file__), 'ymir.json')
    NexusContainer.from_template_file(path)


@pytest.fixture
def ev44_generator() -> RandomEV44Generator:
    return random_ev44_generator(
        detector_numbers=DetectorNumberCandidates(list(range(100)))
    )


def test_ev44_generator_size(ev44_generator: RandomEV44Generator):
    ef_rate = int(10_000 / 14)  # default event rate / frame rate
    events = next(ev44_generator)
    assert int(ef_rate * 0.99) <= len(events['time_of_flight']) <= int(ef_rate * 1.01)
    assert len(events['pixel_id']) == len(events['time_of_flight'])
    assert len(events['reference_time']) == len(events['reference_time_index'])


def test_ev44_generator_reference_time(ev44_generator: RandomEV44Generator):
    events = next(ev44_generator)
    next_events = next(ev44_generator)
    assert events['reference_time'][0] < next_events['reference_time'][0]
