# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os

import pytest

from beamlime.applications._nexus_helpers import NexusContainer
from beamlime.applications._random_data_providers import (
    DetectorName,
    DetectorNumberCandidates,
    RandomEV44Generator,
    random_ev44_generator,
)


@pytest.fixture
def nexus_container() -> NexusContainer:
    path = os.path.join(os.path.dirname(__file__), 'ymir.json')
    return NexusContainer.from_template_file(path)


@pytest.fixture
def ev44_generator() -> RandomEV44Generator:
    return random_ev44_generator(
        source_name=DetectorName('test'),
        detector_numbers=DetectorNumberCandidates(list(range(100))),
    )


def test_ev44_generator_size(ev44_generator: RandomEV44Generator):
    ef_rate = int(10_000 / 14)  # default event rate / frame rate
    events = next(ev44_generator)

    assert events['source_name'] == 'test'
    assert int(ef_rate * 0.99) <= len(events['time_of_flight']) <= int(ef_rate * 1.01)
    assert len(events['pixel_id']) == len(events['time_of_flight'])
    assert len(events['reference_time']) == len(events['reference_time_index'])


def test_ev44_generator_reference_time(ev44_generator: RandomEV44Generator):
    events = next(ev44_generator)
    next_events = next(ev44_generator)
    assert events['reference_time'][0] < next_events['reference_time'][0]


def test_ev44_module_parsing(nexus_container: NexusContainer) -> None:
    assert len(nexus_container.modules['ev44']) == 2
    assert len(nexus_container.detectors) == 2
    for i in range(2):
        assert f"hypothetical_detector_{i}" in nexus_container.modules['ev44']


@pytest.mark.parametrize('detector_id', [0, 1])
def test_ev44_module_insert(nexus_container: NexusContainer, detector_id: int) -> None:
    # create a hypothetical event
    ev44 = dict(
        source_name=DetectorName(f"hypothetical_detector_{detector_id}"),
        reference_time=[0.0],
        reference_time_index=[0],
        time_of_flight=[0.0, 0.1, 0.2],
        pixel_id=[i + detector_id for i in range(3)],
    )
    sub_dataset_names = list(key for key in ev44.keys() if key != 'source_name')

    container = nexus_container.modules['ev44'][f"hypothetical_detector_{detector_id}"]
    # check that the container is empty
    for sub_dataset_name in sub_dataset_names:
        assert len(container.sub_datasets[sub_dataset_name].config_dict['values']) == 0

    for i_insert in range(1, 4):
        # insert the hypothetical event
        nexus_container.insert_ev44(ev44)
        # check that the container is filled
        for sub_dataset_name in sub_dataset_names:
            assert all(
                container.sub_datasets[sub_dataset_name].config_dict['values']
                == ev44[sub_dataset_name] * i_insert
            )
