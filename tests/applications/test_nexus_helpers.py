# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import hashlib
import json
import pathlib

import pytest

from beamlime.applications._nexus_helpers import (
    combine_store_and_structure,
    find_nexus_structure,
    iter_nexus_structure,
    merge_message_into_store,
)
from beamlime.applications._random_data_providers import (
    DetectorName,
    DetectorNumberCandidates,
    EventRate,
    FrameRate,
    RandomEV44Generator,
    random_ev44_generator,
)
from beamlime.applications.daemons import fake_event_generators
from tests.applications.data import ymir  # noqa: F401


@pytest.fixture
def ev44_generator() -> RandomEV44Generator:
    return random_ev44_generator(
        source_name=DetectorName('test'),
        detector_numbers=DetectorNumberCandidates(list(range(100))),
        event_rate=EventRate(10_000),
        frame_rate=FrameRate(14),
    )


@pytest.fixture
def ymir_ev44_generator(ymir):  # noqa: F811
    generators = fake_event_generators(
        ymir,
        event_rate=100,
        frame_rate=14,
    )

    def events():
        for values in zip(*generators.values()):
            for value in values:
                yield value

    return events()


def test_find_index_general():
    first = {'name': 'b0'}
    nested_obj = {
        'children': [
            first,
            {'name': 'b1'},
            {'name': 'b2'},
        ],
        'name': 'a',
    }
    assert find_nexus_structure(nested_obj, ('b0',)) == first


def test_invalid_nexus_template_multiple_module_placeholders() -> None:
    with open(pathlib.Path(__file__).parent / "multiple_modules_datagroup.json") as f:
        nexus_structure = json.load(f)

    with pytest.raises(
        ValueError, match="Multiple modules found in the same data group."
    ):
        merge_message_into_store(
            {}, nexus_structure, ('ev44', {'source_name': 'ymir_00'})
        )


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


def test_ev44_module_parsing(ymir, ymir_ev44_generator):  # noqa: F811
    store = {}
    for _, e in zip(range(4), ymir_ev44_generator):
        merge_message_into_store(store, ymir, ('ev44', e))

    assert len(store) == 2
    result = combine_store_and_structure(store, ymir)

    assert 2 == sum(
        1
        for _, c in iter_nexus_structure(result)
        if any(a.get('values') == 'NXdetector' for a in c.get('attributes', ()))
    )
    for _, c in iter_nexus_structure(result):
        if any(a.get('values') == 'NXevent_data' for a in c.get('attributes', ())):
            assert 'module' not in c
            assert 'children' in c
            assert all(v['module'] == 'dataset' for v in c['children'])

    # original unchanged
    for _, c in iter_nexus_structure(ymir):
        if any(a.get('values') == 'NXevent_data' for a in c.get('attributes', ())):
            assert c['children'][0]['module'] == 'ev44'
