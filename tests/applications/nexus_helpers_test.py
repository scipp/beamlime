# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import hashlib
import pathlib
from collections.abc import Generator, Mapping

import numpy as np
import pytest

from beamlime.applications._nexus_helpers import (
    InvalidNexusStructureError,
    StreamModuleKey,
    StreamModuleValue,
    collect_streaming_modules,
    find_nexus_structure,
    iter_nexus_structure,
    merge_message_into_nexus_store,
)
from beamlime.applications._random_data_providers import (
    DetectorName,
    DetectorNumberCandidates,
    EventRate,
    FrameRate,
    RandomEV44Generator,
    nxevent_data_ev44_generator,
    random_ev44_generator,
)
from beamlime.applications.daemons import fake_event_generators


@pytest.fixture()
def ymir_streaming_modules(ymir: dict) -> dict[StreamModuleKey, StreamModuleValue]:
    return collect_streaming_modules(ymir)


def test_iter_nexus_structure() -> None:
    expected_keys = [(), ('a',), ('a', 'c'), ('b',)]
    test_structure = {
        "children": [
            {"config": {"name": "a"}, "children": [{"config": {"name": "c"}}]},
            {"config": {"name": "b"}},
        ]
    }
    keys = [path for path, _ in iter_nexus_structure(test_structure)]
    assert set(expected_keys) == set(keys)


def test_ev44_generator_no_detector_numbers() -> None:
    """Monitors don't have pixel_id."""
    ev44 = random_ev44_generator(
        source_name=DetectorName("test"),
        detector_numbers=None,
        event_rate=EventRate(10_000),
        frame_rate=FrameRate(14),
    )

    events = next(ev44)
    assert events["source_name"] == "test"
    assert (
        int((10_000 // 14) * 0.99)
        <= len(events["time_of_flight"])
        <= int((10000 // 14) * 1.01)  # 1% fluctuation expected
    )
    assert events["pixel_id"] is None
    assert len(events["reference_time"]) == len(events["reference_time_index"])


@pytest.fixture()
def ev44_generator() -> RandomEV44Generator:
    return random_ev44_generator(
        source_name=DetectorName("test"),
        detector_numbers=DetectorNumberCandidates(list(range(100))),
        event_rate=EventRate(10_000),
        frame_rate=FrameRate(14),
    )


@pytest.fixture()
def ymir_ev44_generator(
    ymir_streaming_modules: dict[StreamModuleKey, StreamModuleValue],
    ymir: dict,
) -> Generator[dict, None, None]:
    generators = fake_event_generators(
        streaming_modules=ymir_streaming_modules,
        nexus_structure=ymir,
        event_rate=EventRate(100),
        frame_rate=FrameRate(14),
    )

    def events() -> Generator[dict, None, None]:
        for values in zip(*generators.values(), strict=True):
            yield from values

    return events()


def test_find_index_general() -> None:
    first = {"name": "b0"}
    nested_obj = {
        "children": [
            first,
            {"name": "b1"},
            {"name": "b2"},
        ],
        "name": "a",
    }
    assert find_nexus_structure(nested_obj, ("b0",)) == first


def test_find_nexus_structure_not_found_raises() -> None:
    with pytest.raises(KeyError):
        find_nexus_structure({}, ("b0",))


def test_ymir_detector_template_checksum() -> None:
    """Test that the ymir template with detectors is updated.

    ``ymir_detectors.json`` is modified version of ``ymir.json``
    to include detector data.
    Therefore we keep track of the file via version control.
    This test is for making sure to update the same file
    in the public server after modifying the file.
    """
    from tests.applications.data import get_checksum

    local_ymir_path = pathlib.Path(__file__).parent / "ymir_detectors.json"
    # check md5 sum of the ``local_ymir_path`` file
    with open(local_ymir_path, "rb") as f:
        local_ymir_md5 = f"md5:{hashlib.md5(f.read()).hexdigest()}"  # noqa: S324

    assert local_ymir_md5 == get_checksum("ymir_detectors.json")


def test_ev44_generator_size(ev44_generator: RandomEV44Generator) -> None:
    ef_rate = int(10_000 / 14)  # default event rate / frame rate
    events = next(ev44_generator)

    assert events["source_name"] == "test"
    assert int(ef_rate * 0.99) <= len(events["time_of_flight"]) <= int(ef_rate * 1.01)
    assert events["pixel_id"] is not None
    assert len(events["pixel_id"]) == len(events["time_of_flight"])
    assert len(events["reference_time"]) == len(events["reference_time_index"])


def test_ev44_generator_reference_time(ev44_generator: RandomEV44Generator) -> None:
    events = next(ev44_generator)
    next_events = next(ev44_generator)
    assert events["reference_time"][0] < next_events["reference_time"][0]


def _is_class(partial_structure: Mapping, cls_name: str) -> bool:
    return any(
        a.get("values") == cls_name and a.get("name") == "NX_class"
        for a in partial_structure.get("attributes", ())
    )


def _is_detector(c: Mapping) -> bool:
    return _is_class(c, "NXdetector")


def _is_event_data(c: Mapping) -> bool:
    return _is_class(c, "NXevent_data")


def _find_event_time_zerov_values(c: Mapping) -> np.ndarray:
    return find_nexus_structure(c, ("event_time_zero",))["config"]["values"]


def _find_event_time_offset_values(c: Mapping) -> np.ndarray:
    return find_nexus_structure(c, ("event_time_offset",))["config"]["values"]


def _find_event_index_values(c: Mapping) -> np.ndarray:
    return find_nexus_structure(c, ("event_index",))["config"]["values"]


def _find_event_id_values(c: Mapping) -> np.ndarray:
    return find_nexus_structure(c, ("event_id",))["config"]["values"]


def test_ev44_module_merging(
    ymir_streaming_modules: dict[StreamModuleKey, StreamModuleValue],
    ymir_ev44_generator: Generator[dict, None, None],
) -> None:
    ev44_modules = {
        key: value
        for key, value in ymir_streaming_modules.items()
        if key.module_type == "ev44"
    }
    store = {}
    stored_data = {}
    for _, data_piece in zip(range(2), ymir_ev44_generator, strict=False):
        for key, value in ev44_modules.items():
            merge_message_into_nexus_store(
                module_key=key,
                module_spec=value,
                nexus_store=store,
                data=data_piece,
            )
            stored_data.setdefault(key, []).append(data_piece)

    for key in ev44_modules.keys():
        assert len(store) == len(ev44_modules)
        stored_value = store[key]
        assert _is_event_data(stored_value)
        assert stored_value['name'] == 'ymir_detector_events'
        assert len(stored_value['children']) == 4  # 4 datasets
        # Test event time zero
        event_time_zero_values = _find_event_time_zerov_values(stored_value)
        inserted_event_time_zeros = np.concatenate(
            [d["reference_time"] for d in stored_data[key]]
        )
        assert np.all(event_time_zero_values == inserted_event_time_zeros)
        # Test event time offset
        event_time_offset_values = _find_event_time_offset_values(stored_value)
        inserted_event_time_offsets = np.concatenate(
            [d["time_of_flight"] for d in stored_data[key]]
        )
        assert np.all(event_time_offset_values == inserted_event_time_offsets)
        # Test event id
        event_id_values = _find_event_id_values(stored_value)
        inserted_event_ids = np.concatenate([d["pixel_id"] for d in stored_data[key]])
        assert np.all(event_id_values == inserted_event_ids)
        # Test event index
        # event index values are calculated based on the length of the previous events
        first_event_length = len(stored_data[key][0]["time_of_flight"])
        expected_event_indices = np.array([0, first_event_length])
        event_index_values = _find_event_index_values(stored_value)
        assert np.all(event_index_values == expected_event_indices)


def test_ev44_module_merging_numpy_array_wrapped(
    ymir_streaming_modules: dict[StreamModuleKey, StreamModuleValue],
    ymir_ev44_generator: Generator[dict, None, None],
) -> None:
    ev44_modules = {
        key: value
        for key, value in ymir_streaming_modules.items()
        if key.module_type == "ev44"
    }
    store = {}
    for _, data_piece in zip(range(2), ymir_ev44_generator, strict=False):
        for key, value in ev44_modules.items():
            merge_message_into_nexus_store(
                module_key=key,
                module_spec=value,
                nexus_store=store,
                data=data_piece,
            )

    NUMPY_DATASETS = ("event_id", "event_index", "event_time_offset", "event_time_zero")
    for key in ev44_modules.keys():
        result = store[key]
        for field_name in NUMPY_DATASETS:
            field = find_nexus_structure(result, (field_name,))
            assert isinstance(field["config"]["values"], np.ndarray)
            assert field["config"]["name"] == field_name


def test_nxevent_data_ev44_generator_yields_frame_by_frame() -> None:
    ev44 = nxevent_data_ev44_generator(
        source_name=DetectorName("test"),
        event_id=np.asarray([1, 1, 2, 1, 2, 1]),
        event_index=np.asarray([0, 3, 3, 5]),
        event_time_offset=np.asarray([1, 2, 3, 4, 5, 6]),
        event_time_zero=np.asarray([1, 2, 3]),
    )

    events = next(ev44)
    assert events["source_name"] == "test"
    assert events["reference_time"] == [1]
    assert events["reference_time_index"] == [0]
    assert np.all(events["time_of_flight"] == [1, 2, 3])
    assert np.all(events["pixel_id"] == [1, 1, 2])

    events = next(ev44)
    assert events["source_name"] == "test"
    assert events["reference_time"] == [2]
    assert events["reference_time_index"] == [0]  # always 0
    assert len(events["time_of_flight"]) == 0
    assert events["pixel_id"] is not None
    assert len(events["pixel_id"]) == 0

    events = next(ev44)
    assert events["source_name"] == "test"
    assert events["reference_time"] == [3]
    assert events["reference_time_index"] == [0]  # always 0
    assert np.all(events["time_of_flight"] == [4, 5])
    assert np.all(events["pixel_id"] == [1, 2])

    with pytest.raises(StopIteration):
        next(ev44)


def test_ev44_merge_no_children_raises() -> None:
    key = StreamModuleKey("ev44", "", "")
    wrong_value = StreamModuleValue(
        path=("",),
        parent={"children": []},
        dtype="int32",
        value_units="km",
    )
    with pytest.raises(
        InvalidNexusStructureError,
        match="Group containing ev44 module should have exactly one child",
    ):
        merge_message_into_nexus_store(
            module_key=key,
            module_spec=wrong_value,
            nexus_store={},
            data={},
        )


def test_ev44_merge_too_many_children_raises() -> None:
    key = StreamModuleKey("ev44", "", "")
    wrong_value = StreamModuleValue(
        path=("",),
        parent={"children": []},
        dtype="int32",
        value_units="km",
    )
    with pytest.raises(
        InvalidNexusStructureError,
        match="Group containing ev44 module should have exactly one child",
    ):
        merge_message_into_nexus_store(
            module_key=key,
            module_spec=wrong_value,
            nexus_store={},
            data={},
        )


@pytest.fixture()
def nexus_template_with_streamed_log(dtype):
    return {
        "name": "the_log_name",
        "children": [
            {
                "module": "f144",
                "config": {
                    "topic": "the_topic",
                    "dtype": dtype,
                    "value_units": "km",
                    "source": "the_source_name",
                },
            }
        ],
    }


def f144_event_generator(shape, dtype):
    generator = (
        (lambda n: np.random.randint(-1000, 1000, (n, *shape)).astype(dtype))
        if np.issubdtype(np.dtype(dtype), np.signedinteger)
        else (lambda n: np.random.randint(0, 1000, (n, *shape)).astype(dtype))
        if np.issubdtype(np.dtype(dtype), np.unsignedinteger)
        else (lambda n: np.random.randn(n, *shape).astype(dtype))
    )
    timestamp = 0
    while True:
        value = generator(np.random.randint(0, 100))
        timestamp += np.random.randint(0, 1000_000_000)
        yield dict(value=value, timestamp=timestamp, source_name="the_source_name")  # noqa: C408


@pytest.mark.parametrize('shape', [(1,), (2,), (2, 2)])
@pytest.mark.parametrize('dtype', ['int32', 'uint32', 'float32', 'float64', 'bool'])
def test_f144_merge(nexus_template_with_streamed_log, shape, dtype):
    modules = collect_streaming_modules(nexus_template_with_streamed_log)
    f144_modules = {
        key: value for key, value in modules.items() if key.module_type == 'f144'
    }
    store = {}
    for key, value in f144_modules.items():
        for _, data in zip(range(10), f144_event_generator(shape, dtype), strict=False):
            merge_message_into_nexus_store(
                module_key=key,
                module_spec=value,
                nexus_store=store,
                data=data,
            )

    expected_key = StreamModuleKey('f144', 'the_topic', 'the_source_name')
    assert expected_key in store
    stored_value = store[expected_key]
    assert len(stored_value['children']) == 2
    values = find_nexus_structure(stored_value, ('value',))
    assert values['module'] == 'dataset'
    times = find_nexus_structure(stored_value, ('time',))
    assert times['module'] == 'dataset'
    assert values['config']['values'].shape[1:] == shape
    assert values['attributes'][0]['values'] == 'km'


def test_f144_merge_no_children_raises():
    key = StreamModuleKey(module_type='f144', topic='', source='')
    wrong_value = StreamModuleValue(
        path=('',),
        parent={'children': []},
        dtype='int32',
        value_units='km',
    )
    with pytest.raises(
        InvalidNexusStructureError,
        match="Group containing f144 module should have exactly one child",
    ):
        merge_message_into_nexus_store(
            module_key=key,
            module_spec=wrong_value,
            nexus_store={},
            data={},
        )


def test_f144_merge_too_many_children_raises():
    key = StreamModuleKey(module_type='f144', topic='', source='')
    wrong_value = StreamModuleValue(
        path=('',),
        parent={'children': [{}, {}]},
        dtype='int32',
        value_units='km',
    )
    with pytest.raises(
        InvalidNexusStructureError,
        match="Group containing f144 module should have exactly one child",
    ):
        merge_message_into_nexus_store(
            module_key=key,
            module_spec=wrong_value,
            nexus_store={},
            data={},
        )


def test_f144_merge_missing_dtype_raises():
    key = StreamModuleKey(module_type='f144', topic='', source='')
    wrong_value = StreamModuleValue(
        path=('',),
        parent={'children': [{}]},
        dtype=None,
        value_units='km',
    )
    with pytest.raises(
        InvalidNexusStructureError,
        match="f144 module spec should have dtype and value_units",
    ):
        merge_message_into_nexus_store(
            module_key=key,
            module_spec=wrong_value,
            nexus_store={},
            data={},
        )


def test_f144_merge_missing_value_units_raises():
    key = StreamModuleKey(module_type='f144', topic='', source='')
    wrong_value = StreamModuleValue(
        path=('',),
        parent={'children': [{}]},
        dtype='int32',
        value_units=None,
    )
    with pytest.raises(
        InvalidNexusStructureError,
        match="f144 module spec should have dtype and value_units",
    ):
        merge_message_into_nexus_store(
            module_key=key,
            module_spec=wrong_value,
            nexus_store={},
            data={},
        )


@pytest.fixture()
def nexus_template_with_streamed_tdct():
    return {
        "name": "chopper_3",
        "children": [
            {
                "module": "tdct",
                "config": {"source": "chopper_3", "topic": "dream_choppers"},
            },
        ],
    }


def tdct_event_generator():
    max_last_timestamp = 0
    counter = 0
    while True:
        timestamps = (
            np.random.randint(0, 1000_000, 100, dtype='uint64').cumsum()
            + max_last_timestamp
        )
        yield dict(  # noqa: C408
            timestamps=timestamps,
            sequence_counter=counter,
            name="chopper_3",
            # name should match the ``source`` in the module config(StreamModuleKey)
        )
        counter += 1
        max_last_timestamp = timestamps.max()


def test_tdct_merge(nexus_template_with_streamed_tdct: dict):
    modules = collect_streaming_modules(nexus_template_with_streamed_tdct)
    tdct_modules = {
        key: value for key, value in modules.items() if key.module_type == 'tdct'
    }
    store = {}
    for key, value in tdct_modules.items():
        for _, data in zip(range(10), tdct_event_generator(), strict=False):
            merge_message_into_nexus_store(
                module_key=key,
                module_spec=value,
                nexus_store=store,
                data=data,
            )

    expected_key = StreamModuleKey('tdct', 'dream_choppers', 'chopper_3')
    assert expected_key in store
    tdct = store[expected_key]
    assert tdct['module'] == 'dataset'
    assert np.issubdtype(
        tdct['config']['values'].dtype, np.dtype(tdct['config']['dtype'])
    )
