# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import hashlib
import json
import pathlib
from typing import Generator, Mapping

import numpy as np
import pytest

from beamlime.applications._nexus_helpers import (
    combine_nexus_group_store_and_structure,
    find_nexus_structure,
    iter_nexus_structure,
    merge_message_into_nexus_group_store,
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
        for values in zip(*generators.values()):
            for value in values:
                yield value

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


def test_invalid_nexus_template_multiple_module_placeholders() -> None:
    with open(pathlib.Path(__file__).parent / "multiple_modules_datagroup.json") as f:
        nexus_structure = json.load(f)

    with pytest.raises(
        ValueError, match="Multiple modules found in the same data group."
    ):
        merge_message_into_nexus_group_store(
            structure=nexus_structure,
            nexus_group_store={},
            module_name="ev44",
            data_piece={"source_name": "ymir_00"},
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
        local_ymir_md5 = f"md5:{hashlib.md5(f.read()).hexdigest()}"

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


def test_ev44_module_parsing(ymir: dict) -> None:
    result = combine_nexus_group_store_and_structure(
        structure=ymir, nexus_group_store={}
    )

    detectors = [c for _, c in iter_nexus_structure(result) if _is_detector(c)]
    assert len(detectors) == 2  # We inserted 2 detectors in the ymir_detectors template


def test_ev44_module_merging(
    ymir: dict, ymir_ev44_generator: Generator[dict, None, None]
) -> None:
    store = {}
    for _, data_piece in zip(range(4), ymir_ev44_generator):
        merge_message_into_nexus_group_store(
            structure=ymir,
            nexus_group_store=store,
            module_name="ev44",
            data_piece=data_piece,
        )
    result = combine_nexus_group_store_and_structure(
        structure=ymir, nexus_group_store=store
    )

    for nx_event in (c for _, c in iter_nexus_structure(result) if _is_event_data(c)):
        assert "children" in nx_event
        assert all(v["module"] == "dataset" for v in nx_event["children"])


def test_ev44_module_merging_numpy_array_wrapped(ymir, ymir_ev44_generator) -> None:
    store = {}
    for _, data_piece in zip(range(4), ymir_ev44_generator):
        merge_message_into_nexus_group_store(
            structure=ymir,
            nexus_group_store=store,
            module_name="ev44",
            data_piece=data_piece,
        )
    result = combine_nexus_group_store_and_structure(
        structure=ymir, nexus_group_store=store
    )
    NUMPY_DATASETS = ("event_id", "event_index", "event_time_offset", "event_time_zero")

    for nx_event in (c for _, c in iter_nexus_structure(result) if _is_event_data(c)):
        assert all(
            isinstance(v["config"]["values"], np.ndarray)
            for v in nx_event["children"]
            if v["config"]["name"] in NUMPY_DATASETS
        )


def test_ev44_module_parsing_original_unchanged(ymir, ymir_ev44_generator) -> None:
    store = {}
    for _, data_piece in zip(range(4), ymir_ev44_generator):
        merge_message_into_nexus_group_store(
            structure=ymir,
            nexus_group_store=store,
            module_name="ev44",
            data_piece=data_piece,
        )
    combine_nexus_group_store_and_structure(structure=ymir, nexus_group_store=store)
    # original unchanged
    for nx_event in (c for _, c in iter_nexus_structure(ymir) if _is_event_data(c)):
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
