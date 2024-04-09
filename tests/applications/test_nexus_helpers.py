# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import hashlib
import pathlib

import pytest

from beamlime.applications._nexus_helpers import (
    NexusContainer,
    find_index,
    nested_shallow_copy,
)
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


def test_find_index_general() -> None:
    from beamlime.applications._nexus_helpers import find_index

    nested_obj = {
        'children': [
            {'name': 'b0'},
            {'name': 'b1'},
            {'name': 'b2'},
        ],
        'name': 'a',
    }

    assert find_index(nested_obj, 'children', lambda obj: obj['name'] == 'b1') == (
        'children',
        1,
    )


def test_find_index_filters() -> None:
    def child_filter(k: str, v: dict[str, str]):
        return k.startswith('child') and v['name'].startswith('B')

    def grand_child_filter(obj: dict[str, str]):
        return obj['name'].startswith('Green')

    nested_obj = {
        "child_1": {
            "name": "Apple",
            "children": [
                {"name": "Red Apple"},
                {"name": "Green Apple"},
                {"name": "Yellow Apple"},
            ],
        },
        "child_2": {
            "name": "Banana",
            "children": [
                {"name": "Yellow Banana"},
                {"name": "Green Banana"},
                {"name": "Brown Banana"},
            ],
        },
        "sibling_1": {
            "name": "Cherry",
            "children": [
                {"name": "Red Cherry"},
                {"name": "Green Cherry"},
                {"name": "Yellow Cherry"},
            ],
        },
    }

    assert find_index(nested_obj, child_filter, "children", grand_child_filter) == (
        'child_2',
        'children',
        1,
    )


def test_find_index_lambda_filters() -> None:
    nested_obj = {
        'children': [
            {'name': 'b0'},
            {'name': 'b1'},
            {'name': 'b2'},
        ],
        'name': 'a',
    }

    assert find_index(
        nested_obj, lambda _, v: len(v) == 3, lambda obj: obj['name'] == 'b1'
    ) == ('children', 1)


def test_find_index_multiple_matches() -> None:
    nested_obj = {
        'children': [
            {'name': 'b1'},
            {'name': 'b1'},
            {'name': 'b2'},
        ],
        'name': 'a',
    }

    assert find_index(nested_obj, 'children', lambda obj: obj['name'] == 'b1') == (
        'children',
        0,  # Always find the first match
    )


def test_nested_shallow_copy() -> None:
    original = dict(a=dict(b0=dict(c=1), b1=dict(c=1)))
    # 0-depth copy
    copied = nested_shallow_copy(original)
    assert copied == original
    assert copied is not original
    assert copied['a'] is original['a']

    # 1-depth copy up to 'a'
    copied = nested_shallow_copy(original, 'a')
    assert copied == original
    assert copied is not original
    assert copied['a'] == original['a']
    assert copied['a'] is not original['a']
    assert copied['a']['b0'] is original['a']['b0']
    assert copied['a']['b1'] is original['a']['b1']

    # 2-depth copy up to 'b'
    copied = nested_shallow_copy(original, 'a', 'b0')
    assert copied == original
    assert copied is not original
    assert copied['a'] == original['a']
    assert copied['a'] is not original['a']
    assert copied['a']['b0'] == original['a']['b0']
    assert copied['a']['b0'] is not original['a']['b0']
    assert copied['a']['b1'] is original['a']['b1']

    # 3-depth copy up to 'c'
    copied = nested_shallow_copy(original, 'a', 'b0', 'c')
    copied['a']['b0']['c'] = 2
    assert copied['a']['b0']['c'] != original['a']['b0']['c']


def test_invalid_nexus_template_multiple_module_placeholders() -> None:
    import json

    from beamlime.applications._nexus_helpers import check_multi_module_datagroup

    invalid_nexus_template_path = (
        pathlib.Path(__file__).parent / "multiple_modules_datagroup.json"
    )
    with pytest.raises(
        ValueError, match="Multiple modules found in the same data group."
    ):
        NexusContainer.from_template_file(invalid_nexus_template_path)

    with pytest.raises(
        ValueError, match="Multiple modules found in the same data group."
    ):
        invalid_nexus_dict = json.loads(invalid_nexus_template_path.read_text())
        check_multi_module_datagroup(invalid_nexus_dict)


def test_ymir_detector_template_checksum() -> None:
    """Test that the ymir template with detectors is updated.

    ``ymir_detectors.json`` is modified version of ``ymir.json``
    to include detector data.
    Therefore we keep track of the file via version control.
    This test is for making sure to update the same file
    in the public server after modifying the file.
    """
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
