# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import hashlib
import json
import pathlib
from collections.abc import Generator, Mapping

import numpy as np
import pytest

from beamlime.applications._nexus_helpers import (
    Nexus,
    NexusGroup,
    collect_modules,
    find_nexus_structure,
    iter_nexus_structure,
    merge_message,
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
def ymir_ev44_generator(ymir: dict) -> Generator[dict, None, None]:
    generators = fake_event_generators(
        ymir,
        event_rate=EventRate(100),
        frame_rate=FrameRate(14),
    )

    def events() -> Generator[dict, None, None]:
        for values in zip(*generators.values(), strict=True):
            yield from values

    return events()


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


def test_invalid_nexus_template_multiple_module_placeholders() -> None:
    with open(pathlib.Path(__file__).parent / "multiple_modules_datagroup.json") as f:
        nexus_structure = json.load(f)
    with pytest.raises(ValueError, match="should have exactly one child"):
        merge_message(
            store={},
            modules=collect_modules(nexus_structure)['ev44'],
            data={"source_name": "ymir_00"},
            module_name='ev44',
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


def _is_event_data(c: Mapping) -> bool:
    return _is_class(c, "NXevent_data")


def test_ev44_module_parsing(ymir: NexusGroup) -> None:
    assert (
        len([c for _, c in collect_modules(ymir)["ev44"].items() if _is_event_data(c)])
        == 2
    )  # We inserted 2 detectors in the ymir_detectors template


def test_f144_module_parsing(ymir: NexusGroup) -> None:
    assert (
        len(
            [
                c
                for _, c in collect_modules(ymir)["f144"].items()
                if _is_class(c, "NXlog")
            ]
        )
        == 17
    )  # There are 17 f144 modules in ymir example.


def _get_child(group: NexusGroup, child_name: str) -> Nexus:
    for child in group["children"]:
        if child.get("config", {}).get("name", None) == child_name:
            return child
    raise KeyError(child_name)


def _get_child_values(group: NexusGroup, child_name: str) -> np.ndarray:
    return _get_child(group, child_name).get('config', {}).get('values', [])


def test_ev44_module_merging(
    ymir: NexusGroup, ymir_ev44_generator: Generator[dict, None, None]
) -> None:
    store = {}
    modules = collect_modules(ymir)["ev44"]
    expected_event_time_offsets = {}
    expected_event_index = {}
    NUM_DETECTORS = 2
    for _, data_piece in zip(range(4), ymir_ev44_generator, strict=False):
        i_detector = _ % NUM_DETECTORS
        merge_message(
            store=store,
            modules=modules,
            data=data_piece,
            module_name="ev44",
        )
        expected_event_index.setdefault(i_detector, []).append(
            len(expected_event_time_offsets.get(i_detector, []))
        )
        expected_event_time_offsets.setdefault(i_detector, []).extend(
            data_piece["time_of_flight"]
        )

    for i_nx_event, nx_event in enumerate(store.values()):
        assert "children" in nx_event
        assert all(v["module"] == "dataset" for v in nx_event["children"])
        # Test if all event data is appended
        assert all(
            _get_child_values(nx_event, "event_time_offset")
            == expected_event_time_offsets[i_nx_event]
        )
        # Test if all event index is adjusted correctly
        assert all(
            _get_child_values(nx_event, "event_index")
            == expected_event_index[i_nx_event]
        )


def test_ev44_module_merging_numpy_array_wrapped(ymir, ymir_ev44_generator) -> None:
    NUMPY_DATASETS = ("event_id", "event_index", "event_time_offset", "event_time_zero")
    store = {}
    modules = collect_modules(ymir)["ev44"]
    for _, data_piece in zip(range(4), ymir_ev44_generator, strict=False):
        merge_message(
            store=store,
            modules=modules,
            data=data_piece,
            module_name="ev44",
        )
    for nx_event in store.values():
        assert all(
            isinstance(_get_child_values(nx_event, child_name), np.ndarray)
            for child_name in NUMPY_DATASETS
        )


def test_ev44_module_parsing_original_unchanged(ymir, ymir_ev44_generator) -> None:
    store = {}
    modules = collect_modules(ymir)["ev44"]
    for _, data_piece in zip(range(4), ymir_ev44_generator, strict=False):
        merge_message(
            store=store,
            modules=modules,
            data=data_piece,
            module_name="ev44",
        )
    # original unchanged
    for nx_event in (c for c in modules.values() if _is_event_data(c)):
        assert len(nx_event["children"]) == 1
        assert nx_event["children"][0]["module"] == "ev44"


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


@pytest.fixture()
def nexus_template_with_streamed_log(dtype):
    return {
        "name": "the_log_name",
        "children": [
            {
                "module": "f144",
                "config": {
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
def test_f144(nexus_template_with_streamed_log, shape, dtype):
    store = {}
    modules = collect_modules(nexus_template_with_streamed_log)['f144']
    for _, data in zip(range(10), f144_event_generator(shape, dtype), strict=False):
        merge_message(
            store=store,
            modules=modules,
            data=data,
            module_name="f144",
        )

    assert len(store['']['children']) == 2
    values = find_nexus_structure(store[""], ('value',))
    assert values['module'] == 'dataset'
    times = find_nexus_structure(store[""], ('time',))
    assert times['module'] == 'dataset'
    assert values['config']['values'].shape[1:] == shape
    assert values['attributes'][0]['values'] == 'km'


@pytest.fixture()
def nexus_template_with_streamed_tdct():
    return {
        "name": "chopper_3",
        "children": [
            {
                "module": "tdct",
                "config": {"source": "source", "topic": "dream_choppers"},
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
        )
        counter += 1
        max_last_timestamp = timestamps.max()


def test_tdct(nexus_template_with_streamed_tdct):
    store = {}
    modules = collect_modules(nexus_template_with_streamed_tdct)["tdct"]
    with pytest.raises(NotImplementedError):
        merge_message(
            store=store,
            modules=modules,
            data=next(tdct_event_generator()),
            module_name="tdct",
        )
