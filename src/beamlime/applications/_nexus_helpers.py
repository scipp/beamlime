# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable, Iterable, Mapping
from functools import partial
from typing import Any, Literal, TypeAlias, TypedDict, TypeVar, cast

import numpy as np


class NexusDataset(TypedDict):
    """Represents a static Nexus dataset in a nexus template"""

    module: Literal["dataset"]
    config: dict[str, Any]
    attributes: list[Mapping[str, Any]]


class NexusGroup(TypedDict):
    """Represents a Nexus group in a nexus template"""

    children: list['NexusDataset | NexusGroup']
    name: str
    attributes: list[Mapping[str, Any]]


NexusPath = tuple[str | None, ...]
Nexus: TypeAlias = NexusGroup | NexusDataset
NexusStore = dict[NexusPath, Nexus]
"""Stores nexus datasets or groups that are to be merged into a nexus template"""

ModuleNameType = Literal["ev44", "f144", "tdct"]
"""Names of the streamed modules supported by beamlime"""


class NexusStreamedModule(TypedDict):
    """Definition of streamed dataset or group in a nexus template"""

    module: ModuleNameType
    config: dict[str, Any]


NexusStructure: TypeAlias = 'Nexus | NexusStreamedModule | NexusTemplate'


class NexusTemplate(TypedDict):
    """A nexus template. Nested structure of static datasets or groups,
    or streamed datasets or groups"""

    children: list[NexusStructure]
    name: str | None
    attributes: list[Mapping[str, Any]]


def create_dataset(
    *, name: str, dtype: str, initial_values: Any, unit: str | None = None
) -> NexusDataset:
    """Creates a dataset according to the arguments."""
    dataset: NexusDataset = {
        "module": "dataset",
        "config": {
            "name": name,
            "dtype": dtype,
            "values": initial_values,
        },
        "attributes": [],
    }
    if unit is not None:
        dataset["attributes"].append(
            {
                "name": "units",
                "dtype": "string",
                "values": unit,
            }
        )
    return dataset


DeserializedMessage = Mapping
"""Deserialized message from one of the schemas bound to :attr:`~ModuleNameType`."""


def _find_ev44(
    structure: NexusStructure, data: DeserializedMessage
) -> Iterable[NexusPath]:
    source_name = data["source_name"]
    for path, node in iter_nexus_structure(structure):
        if (
            node.get("module") == "ev44"
            and node.get("config", {}).get("source") == source_name
        ):
            yield path[:-1]


def _initialize_ev44(
    structure: NexusStructure, path: NexusPath, data: DeserializedMessage
) -> NexusGroup:
    """
    Params
    ------
    structure:
        Nexus template containing the ev44 module.
    path:
        Path in the structure where the ev44 module was located.
    data:
        A deserialized message corresponding to ``ev44`` schema.

    """
    parent = cast(NexusTemplate, find_nexus_structure(structure, path))
    if len(parent['children']) != 1:
        raise ValueError('Group containing ev44 module should have exactly one child')
    group: NexusGroup = cast(NexusGroup, parent.copy())
    group['children'] = [
        create_dataset(
            name="event_time_zero",
            dtype="int64",
            initial_values=np.asarray([], dtype="int64"),
            unit="ns",
        ),
        create_dataset(
            name="event_time_offset",
            dtype="int32",
            initial_values=np.asarray([], dtype="int32"),
            unit="ns",
        ),
        create_dataset(
            name="event_index",
            dtype="int32",
            initial_values=np.asarray([], dtype="int32"),
        ),
    ]
    if data.get("pixel_id") is not None:
        # ``event_id``(pixel_id) is optional. i.e. Monitor doesn't have pixel ids.
        group['children'].append(
            create_dataset(
                name="event_id",
                dtype="int32",
                initial_values=np.asarray([], dtype="int32"),
            )
        )
    return group


def _merge_ev44(group: NexusGroup, data: DeserializedMessage) -> None:
    """Merges new values from a message into the data group.

    Params
    ------
    group:
        A data group that has a module place holder as a child.
    data:
        New message containing deserialized ev44.

    Side Effects
    ------------
    Each dataset in the children of ``group`` will have new values appended.
    Most of array-like values are simply appended
    but the ``event_index``(``reference_time_index`` from ev44) is increased
    by the number of previous values of ``event_time_offset``
    to find the global ``event_index``.
    ``data`` only has local ``event_index`` which always starts with 0.

    """
    event_time_zero_dataset = cast(
        NexusDataset, find_nexus_structure(group, ("event_time_zero",))
    )
    event_time_zero_dataset["config"]["values"] = np.concatenate(
        (event_time_zero_dataset["config"]["values"], data["reference_time"])
    )
    event_time_offset_dataset = cast(
        NexusDataset, find_nexus_structure(group, ("event_time_offset",))
    )
    original_event_time_offset = event_time_offset_dataset["config"]["values"]
    event_time_offset_dataset["config"]["values"] = np.concatenate(
        (original_event_time_offset, data["time_of_flight"])
    )
    # Increase event index according to the ``original_event_time_index``
    event_index_dataset = cast(
        NexusDataset, find_nexus_structure(group, ("event_index",))
    )
    event_index_dataset["config"]["values"] = np.concatenate(
        (
            event_index_dataset["config"]["values"],
            data["reference_time_index"] + len(original_event_time_offset),
        )
    )
    if data.get("pixel_id") is not None:  # Pixel id is optional.
        event_id_dataset = cast(
            NexusDataset, find_nexus_structure(group, ("event_id",))
        )
        event_id_dataset["config"]["values"] = np.concatenate(
            (event_id_dataset["config"]["values"], data["pixel_id"])
        )


def _find_f144(
    structure: NexusStructure, data: DeserializedMessage
) -> Iterable[NexusPath]:
    source_name = data["source_name"]
    for path, node in iter_nexus_structure(structure):
        if (
            node.get("module") == "f144"
            and node.get("config", {}).get("source") == source_name
        ):
            yield path[:-1]


def _initialize_f144(
    structure: NexusStructure, path: NexusPath, data: DeserializedMessage
) -> NexusGroup:
    parent = cast(NexusTemplate, find_nexus_structure(structure, path))
    if len(parent['children']) != 1:
        raise ValueError('Group containing f144 module should have exactly one child')
    module = cast(NexusStreamedModule, parent['children'][0])
    group: NexusGroup = cast(NexusGroup, parent.copy())
    group["children"] = [
        create_dataset(
            name="time",
            dtype="int64",
            initial_values=np.asarray([], dtype="int64"),
            unit="ns",
        ),
        create_dataset(
            name="value",
            dtype=module["config"]["dtype"],
            initial_values=np.empty(
                (0, *data['value'].shape[1:]), dtype=module["config"]["dtype"]
            ),
            unit=module["config"]["value_units"],
        ),
    ]
    return group


def _merge_f144(group: NexusGroup, data: DeserializedMessage) -> None:
    time = cast(NexusDataset, find_nexus_structure(group, ("time",)))
    time["config"]["values"] = np.concatenate(
        (time["config"]["values"], [data["timestamp"]])
    )
    value = cast(NexusDataset, find_nexus_structure(group, ("value",)))
    value["config"]["values"] = np.concatenate(
        (value["config"]["values"], data["value"])
    )


def _find_tdct(
    structure: NexusStructure, data: DeserializedMessage
) -> Iterable[NexusPath]:
    name = data["name"]
    for path, node in iter_nexus_structure(structure):
        if node.get("name") == name:
            for _, child in iter_nexus_structure(node):
                if child.get("module") == "tdct":
                    yield (*path, 'top_dead_center')


def _initialize_tdct(
    structure: NexusStructure, path: NexusPath, data: DeserializedMessage
) -> NexusDataset:
    return create_dataset(
        name="top_dead_center",
        dtype="uint64",
        initial_values=np.asarray([], dtype="uint64"),
        unit="ns",
    )


def _merge_tdct(top_dead_center: NexusDataset, data: DeserializedMessage) -> None:
    top_dead_center["config"]["values"] = np.concatenate(
        (top_dead_center["config"]["values"], data["timestamps"])
    )


def _node_name(n: NexusStructure):
    """Defines the name of a nexus tree branch or leaf"""
    config = n.get("config", {})
    return n.get("name", config.get("name"))


def iter_nexus_structure(
    structure: NexusStructure, root: NexusPath | None = None
) -> Iterable[tuple[NexusPath, NexusStructure]]:
    """Visits all branches and leafs in the nexus tree"""
    path = (*root, _node_name(structure)) if root is not None else ()
    yield path, structure
    for child in structure.get("children", []):
        yield from iter_nexus_structure(child, root=path)


def find_nexus_structure(structure: NexusStructure, path: NexusPath) -> NexusStructure:
    """Returns the branch or leaf associated with `path`, or None if not found"""
    if len(path) == 0:
        return structure
    head, *tail = path
    for child in structure.get("children", ()):
        if head == _node_name(child):
            return find_nexus_structure(child, tuple(tail))
    raise KeyError(f"Path {path} not found in the nexus structure.")


DatasetOrGroup = TypeVar("DatasetOrGroup", NexusDataset, NexusGroup)


def _merge_message_into_nexus_store(
    structure: NexusStructure,
    nexus_store: NexusStore,
    data: DeserializedMessage,
    find_insert_paths: Callable[
        [NexusStructure, DeserializedMessage], Iterable[NexusPath]
    ],
    initialize: Callable[
        [NexusStructure, NexusPath, DeserializedMessage], DatasetOrGroup
    ],
    merge_message: Callable[[DatasetOrGroup, DeserializedMessage], None],
) -> None:
    """Bridge function to merge a message into the store.

    This bridge was needed since different modules
    have different ways to match the paths and merge the data.

    Parameters
    ----------
    structure:
        The nexus structure.
    nexus_store:
        The module store that holds the data to be merged in the template.
    data:
        The content of the message.
    find_insert_paths:
        A function that returns the paths in the structure where the
        message should be merged.
    initialize:
        A function that initializes the dataset / group.
        *Initialize is done only when the relevant message arrives.*
        It is because we should distinguish between empty dataset and
        unexpectedly-not-receiving data.
        It also depends on the ``data`` to handle optional fields.
    merge_message:
        A function that merges the message into the initialized nexus dataset / group.

    Side Effects
    ------------
    The ``nexus_group_store`` is updated with the merged data.

    """
    for path in find_insert_paths(structure, data):
        if path not in nexus_store:
            nexus_store[path] = initialize(structure, path, data)
        merge_message(cast(DatasetOrGroup, nexus_store[path]), data)


def merge_message_into_nexus_store(
    *,
    structure: NexusStructure,
    nexus_store: NexusStore,
    data: DeserializedMessage,
    module_name: ModuleNameType,
):
    """Merges message into the associated nexus group.

    If there are multiple paths that match the message,
    all paths will be used.
    In principle, there should only be one path that matches the message.

    But the nexus template does not guarantee that there is a unique matching module.
    There are still some nexus templates that contain multi-matching
    module place holders (i.e. same ``source_name`` in ev44 module).

    In this case, it is more safe to use all paths
    so that no data is lost in the data reduction,
    even though it means unnecessary memory usage
    and slower in the grouping/binning process
    (``N`` times slower where ``N`` is number of duplicating modules).

    For example, if we store all data in to every detector data bank,
    then irrelevant data points will not affect the data reduction
    once the data is grouped by the pixel ids.
    However, if we choose to store the data in only one of the detector data banks,
    then the data of the rest of detectors will be lost.
    """
    merge = partial(_merge_message_into_nexus_store, structure, nexus_store, data)
    if module_name == "ev44":
        merge(
            _find_ev44,
            _initialize_ev44,
            _merge_ev44,
        )
    elif module_name == "f144":
        merge(
            _find_f144,
            _initialize_f144,
            _merge_f144,
        )
    elif module_name == "tdct":
        merge(
            _find_tdct,
            _initialize_tdct,
            _merge_tdct,
        )
    else:
        raise NotImplementedError


def combine_nexus_store_and_structure(
    structure: NexusStructure, nexus_store: NexusStore
) -> Nexus:
    """Creates a new nexus structure, replacing the stream modules
    with the datasets in `store`, while avoiding
    to copy data from `structure` if unnecessary"""
    if len(nexus_store) == 0:
        return cast(Nexus, structure)
    if () in nexus_store:
        return nexus_store[()]

    new = structure.copy()
    if "children" in structure:
        new["children"] = [
            combine_nexus_store_and_structure(
                structure=child,
                nexus_store={
                    tuple(tail): node
                    for (head, *tail), node in nexus_store.items()
                    if head == _node_name(child)
                },
            )
            for child in structure.get("children", ())
            # Filter stream modules
            if "module" not in child or child["module"] == "dataset"
        ]
        # Here we add children that were not in the template
        # but should be added according to the nexus_store.
        for path, value in nexus_store.items():
            if len(path) == 1:
                # Don't replace existing values, they were added in the previous step
                try:
                    find_nexus_structure(new, path)
                except KeyError:
                    new["children"].append(value)

    return new
