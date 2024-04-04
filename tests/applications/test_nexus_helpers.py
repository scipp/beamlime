# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pathlib

import pytest

from beamlime.applications._nexus_helpers import NexusContainer
from beamlime.applications._random_data_providers import (
    DetectorName,
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
        source_name=DetectorName('test'),
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

    from tests.applications.data import get_checksum

    local_ymir_path = pathlib.Path(__file__).parent / 'ymir_detectors.json'
    # check md5 sum of the ``local_ymir_path`` file
    with open(local_ymir_path, 'rb') as f:
        local_ymir_md5 = f"md5:{hashlib.md5(f.read()).hexdigest()}"

    assert local_ymir_md5 == get_checksum('ymir_detectors.json')


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


def test_ev44_module_parsing(ymir_detectors_container: NexusContainer) -> None:
    assert len(ymir_detectors_container.modules['ev44']) == 2
    assert len(ymir_detectors_container.detectors) == 2
    for i in range(2):
        assert f"ymir_{i}" in ymir_detectors_container.modules['ev44']


def help_ev44_module_insert_test(
    container: NexusContainer, detector_id: int, detector_name_prefix: str
) -> None:
    from beamlime.applications._nexus_helpers import nested_dict_getitem

    sub_datagroup_recipe = container.modules['ev44'][
        f"{detector_name_prefix}_{detector_id}"
    ]

    # Save the original dictionary
    original_dict = container.nexus_dict

    # Check that the sub dataset is empty
    original_sub_dataset_list = nested_dict_getitem(
        container.nexus_dict, *sub_datagroup_recipe.target_path
    )
    assert len(original_sub_dataset_list) == 1  # Module placeholder

    # Create a hypothetical event
    ev44 = dict(
        source_name=DetectorName(f"{detector_name_prefix}_{detector_id}"),
        reference_time=[0.0],
        reference_time_index=[0],
        time_of_flight=[0.0, 0.1, 0.2],
        pixel_id=[i + detector_id for i in range(3)],
    )
    sub_dataset_names = list(key for key in ev44.keys() if key != 'source_name')

    # Insert the hypothetical event
    container.insert_ev44(ev44)

    # Check the original dictionary is not modified
    assert container.nexus_dict is not original_dict
    original_sub_dataset_list = nested_dict_getitem(
        original_dict, *sub_datagroup_recipe.target_path
    )
    assert len(original_sub_dataset_list) == 1  # Module placeholder

    # Check if the sub-datasets are populated in the shallow-copied dictionary
    sub_dataset_list = nested_dict_getitem(
        container.nexus_dict, *sub_datagroup_recipe.target_path
    )
    populated_names = [
        sub_dataset['config']['name'] for sub_dataset in sub_dataset_list
    ]
    assert set(populated_names) == set(sub_dataset_names)
    # Check if the inserted event is correct
    for sub_dataset in sub_dataset_list:
        assert sub_dataset['config']['values'] == ev44[sub_dataset['config']['name']]


def test_ev44_module_insert(ymir_detectors_container: NexusContainer) -> None:
    for detector_id in range(2):
        help_ev44_module_insert_test(ymir_detectors_container, detector_id, 'ymir')


def test_ev44_module_insert_loki(loki_container: NexusContainer) -> None:
    # create a hypothetical event
    for detector_id in range(8):
        help_ev44_module_insert_test(loki_container, detector_id, 'loki')
