# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable, Generator, Mapping
from functools import partial
from typing import Any, Literal, TypeAlias, TypedDict, TypeGuard, cast, get_args

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


NexusPath = tuple[str, ...]
Nexus: TypeAlias = NexusGroup | NexusDataset

ModuleNameType = Literal["ev44", "f144", "tdct"]
"""Names of the streamed modules supported by beamlime"""


class NexusStreamedModule(TypedDict):
    """Definition of streamed dataset or group in a nexus template"""

    module: ModuleNameType
    config: dict[str, Any]


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

ModuleRegistry = dict[str, NexusGroup]


def _find_ev44(
    modules: ModuleRegistry, data: DeserializedMessage
) -> Generator[tuple[str, NexusGroup]]:
    source_name = data["source_name"]
    for module_key, module_parent in modules.items():
        if (
            (children := module_parent.get("children")) is not None
            and len(children) == 1  # ev44 placeholder should be the only child
            and (module := children[0]).get("module") == "ev44"
            and module.get("config", {}).get("source") == source_name
        ):
            yield (module_key, module_parent)


def _initialize_ev44(
    parent: NexusGroup, data: DeserializedMessage, _: NexusStreamedModule
) -> NexusGroup:
    """Initialize empty datasets for ev44 module.

    Params
    ------
    parent:
        A ``detector`` group containing ev44 module.
    data:
        A deserialized message corresponding to ``ev44`` schema.

    """
    group: NexusGroup = parent.copy()
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
    modules: ModuleRegistry, data: DeserializedMessage
) -> Generator[tuple[str, NexusGroup]]:
    source_name = data["source_name"]
    for module_key, module_parent in modules.items():
        if (
            (children := module_parent.get("children")) is not None
            and len(children) == 1  # f144 placeholder should be the only child
            and (module := children[0]).get("module") == "f144"
            and module.get("config", {}).get("source") == source_name
        ):
            yield (module_key, module_parent)


def _initialize_f144(
    parent: NexusGroup, data: DeserializedMessage, module: NexusStreamedModule
) -> NexusGroup:
    group = parent.copy()
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
    modules: ModuleRegistry, data: DeserializedMessage
) -> Generator[tuple[str, NexusGroup]]:
    name = data["name"]
    for module_key, module_parent in modules.items():
        if (
            (children := module_parent.get("children")) is not None
            and len(children) == 1  # tdct placeholder should be the only child
            and (module := children[0]).get("module") == "tdct"
            and module.get("config", {}).get("topic") == name
        ):
            yield (module_key, module_parent)


def _initialize_tdct(
    parent: NexusGroup, data: DeserializedMessage, module: NexusStreamedModule
) -> NexusGroup:
    group = parent.copy()
    dtype = module["config"].get("dtype", "uint64")
    group['children'] = [
        create_dataset(
            name=data["name"],
            dtype=dtype,
            initial_values=np.asarray([], dtype=dtype),
            unit=module["config"].get("unit", "ns"),
        )
    ]
    return group


def _merge_tdct(parent: NexusGroup, data: DeserializedMessage) -> None:
    tdct_dataset = cast(NexusDataset, parent["children"][0])
    tdct_dataset["config"]["values"] = np.concatenate(
        (tdct_dataset["config"]["values"], data["timestamps"])
    )


def _node_name(n: Nexus) -> str:
    """Defines the name of a nexus tree branch or leaf"""
    config = n.get("config", {})
    return n.get("name", config.get("name"))


def iter_nexus_structure(
    structure: Nexus, root: NexusPath | None = None
) -> Generator[tuple[NexusPath, Nexus]]:
    """Visits all branches and leafs in the nexus tree"""
    path = (*root, _node_name(structure)) if root is not None else ()
    yield path, structure
    for child in structure.get("children", []):
        yield from iter_nexus_structure(child, root=path)


def find_nexus_structure(structure: Nexus, path: NexusPath) -> Nexus:
    """Returns the branch or leaf associated with `path`, or None if not found"""
    if len(path) == 0:
        return structure
    head, *tail = path
    for child in structure.get("children", ()):
        if head == _node_name(child):
            return find_nexus_structure(child, tuple(tail))
    raise KeyError(f"Path {path} not found in the nexus structure.")


def _merge_message_into_store(
    *,
    store: dict[str, NexusGroup],
    data: DeserializedMessage,
    modules: ModuleRegistry,
    match_module: Callable[
        [ModuleRegistry, DeserializedMessage], Generator[tuple[str, NexusGroup]]
    ],
    initialize: Callable[
        [NexusGroup, DeserializedMessage, NexusStreamedModule], NexusGroup
    ],
    merge_message: Callable[[NexusGroup, DeserializedMessage], None],
) -> None:
    """Bridge function to merge a message into the store.

    This bridge was needed since different modules
    have different ways to match the paths and merge the data.

    Parameters
    ----------
    store:
        The dictionary that holds the data accumulated.
    data:
        The content of the message.
    modules:
        A ``NexusGroup`` or ``NexusDataset`` that has a module place holder in it.
        ``initialize`` can use the matching module to initialize the dataset or a group.
    match_module:
        A function that returns the key-module pair where the message should be merged.
        **It is a generator function because there may be multiple matches**
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
    ``store`` is updated with the merged data.

    """
    for module_key, module_parent in match_module(modules, data):
        if module_key not in store:
            if len(module_parent["children"]) != 1:
                raise ValueError(
                    "The module place holder should have exactly one child."
                    "Multiple children found in the module place holder:"
                    f"{module_parent}"
                )
            module = cast(NexusStreamedModule, module_parent["children"][0])
            store[module_key] = initialize(module_parent, data, module)
        merge_message(store[module_key], data)


def merge_message(
    *,
    store: dict[str, NexusGroup],
    modules: ModuleRegistry,
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
    merge = partial(_merge_message_into_store, store=store, data=data, modules=modules)
    if module_name == "ev44":
        merge(
            match_module=_find_ev44,
            initialize=_initialize_ev44,
            merge_message=_merge_ev44,
        )
    elif module_name == "f144":
        merge(
            match_module=_find_f144,
            initialize=_initialize_f144,
            merge_message=_merge_f144,
        )
    elif module_name == "tdct":
        merge(
            match_module=_find_tdct,
            initialize=_initialize_tdct,
            merge_message=_merge_tdct,
        )
    raise NotImplementedError(f"Module {module_name} is not supported.")


_MODULE_NAMES = get_args(ModuleNameType)


def _is_nexus_group(n: Nexus) -> TypeGuard[NexusGroup]:
    return "children" in n


def _is_module_parent(group: Nexus) -> bool:
    return (
        _is_nexus_group(group)
        and len(children := group.get("children", [])) == 1
        and children[0].get("module") in _MODULE_NAMES
    )


def collect_modules(structure: NexusGroup) -> ModuleRegistry:
    """Collects all modules in the nexus structure"""

    return {
        '/'.join(path): cast(NexusGroup, node)
        for path, node in iter_nexus_structure(structure)
        if _is_module_parent(node)
    }
