# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pytest

from beamlime.applications._nexus_helpers import NexusContainer
from beamlime.applications._random_data_providers import (
    DetectorNumberCandidates,
    EventRate,
    FrameRate,
    RandomEV44Generator,
    random_ev44_generator,
)


@pytest.fixture
def ymir_detectors_container() -> NexusContainer:
    from tests.applications.data import get_path

    return NexusContainer.from_template_file(get_path('ymir_detectors.json'))


@pytest.fixture
def loki_container(large_file_test: bool) -> NexusContainer:
    from tests.applications.data import get_path

    assert large_file_test

    return NexusContainer.from_template_file(get_path('loki.json'))


@pytest.fixture
def ev44_generator() -> RandomEV44Generator:
    return random_ev44_generator(
        detector_numbers=DetectorNumberCandidates(list(range(100))),
        event_rate=EventRate(10_000),
        frame_rate=FrameRate(14),
    )


def test_ymir_detector_template_checksum() -> None:
    """Test that the ymir template with detectors is updated.

    ``ymir_detectors.json`` is modified version of ``ymir.json``
    to include detector data.
    Therefore we keep track of the file via version control.
    This test is for making sure to update the same file
    in the public server after modifying the file.
    """
    import hashlib
    import pathlib

    from tests.applications.data import get_checksum

    local_ymir_path = pathlib.Path(__file__).parent / 'ymir_detectors.json'
    # check md5 sum of the ``local_ymir_path`` file
    with open(local_ymir_path, 'rb') as f:
        local_ymir_md5 = f"md5:{hashlib.md5(f.read()).hexdigest()}"

    assert local_ymir_md5 == get_checksum('ymir_detectors.json')


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
