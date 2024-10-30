# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import pathlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    Any,
    Literal,
    NamedTuple,
    TypeAlias,
    TypedDict,
    TypeGuard,
    TypeVar,
    cast,
    get_args,
)

import numpy as np
import scippnexus as snx
from streaming_data_types.eventdata_ev44 import EventData
from streaming_data_types.logdata_f144 import ExtractedLogData
from streaming_data_types.timestamps_tdct import Timestamps


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


def nexus_path_as_string(path: NexusPath) -> str:
    return "/".join(p for p in path if p is not None)


Nexus: TypeAlias = NexusGroup | NexusDataset


ModuleNameType = Literal["ev44", "f144", "tdct"]
"""Names of the streamed modules supported by beamlime."""

ALL_MODULE_NAMES = get_args(ModuleNameType)
"""All module names supported by beamlime."""


class NexusStreamedModule(TypedDict):
    """Definition of streamed dataset or group in a nexus template"""

    module: ModuleNameType
    config: dict[str, Any]


NexusStructure: TypeAlias = 'Nexus | NexusStreamedModule | NexusTemplate'


class NexusTemplate(TypedDict):
    """A nexus template. Nested structure of static datasets or groups,
    or streamed datasets or groups"""

    children: list["NexusStructure"]
    name: str | None
    attributes: list[Mapping[str, Any]]


class StreamModuleKey(NamedTuple):
    """Hashable key to identify a streamed module in a nexus template"""

    module_type: ModuleNameType
    """Type of the module"""
    topic: str
    """Topic to subscribe to"""
    source: str
    """Name of the source"""


class StreamModuleValue(NamedTuple):
    """Value of a streamed module in a nexus template"""

    path: NexusPath
    """Path in the nexus template where the module is located"""
    parent: dict
    """Parent of the module"""
    dtype: str | None = None
    """Data type of the module. Only used for ``NXlog``"""
    value_units: str | None = None
    """Units of the module. Only used for ``NXlog``"""


@dataclass
class RunStartInfo:
    """Information needed for the start of the live data reduciton.

    It is a subset of ``run_start_message``.
    """

    filename: str
    """The name of the file containing static information."""
    streaming_modules: dict[StreamModuleKey, StreamModuleValue]
    """The streaming modules to be used."""


NexusModuleMap = dict[StreamModuleKey, StreamModuleValue]
"""Map of streamed modules in a nexus template."""
NexusStore = dict[StreamModuleKey, Nexus]
"""Stores nexus datasets or groups that are to be merged."""


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


def is_supported_module_type(module_type: str) -> TypeGuard[ModuleNameType]:
    """Check if the module type is supported by beamlime."""
    return module_type in ALL_MODULE_NAMES


class InvalidNexusStructureError(ValueError):
    """Raised when the nexus structure is invalid."""


def _validate_module_configs(
    key_value_pairs: tuple[tuple[StreamModuleKey, StreamModuleValue], ...],
) -> None:
    """Validate the module configuration in the nexus structure."""
    invalid_module_paths = tuple(
        "/".join(part for part in value.path if part is not None)
        for key, value in key_value_pairs
        if not isinstance(key.topic, str) or not isinstance(key.source, str)
    )
    if len(invalid_module_paths) != 0:
        raise InvalidNexusStructureError(
            "Invalid module place holder(s) found in the nexus structure: ",
            "\n".join(invalid_module_paths),
        )


def _validate_module_keys(
    key_value_pairs: tuple[tuple[StreamModuleKey, StreamModuleValue], ...],
) -> None:
    """Validate the module keys."""
    from collections import Counter

    key_counts = Counter([key for key, _ in key_value_pairs])
    duplicated_keys = [key for key, count in key_counts.items() if count > 1]
    if duplicated_keys:
        raise InvalidNexusStructureError(
            "Duplicate module place holder(s) found in the nexus structure: ",
            duplicated_keys,
        )


def _validate_f144_module_spec(
    module_spec: StreamModuleValue,
) -> None:
    """Validate the f144 module."""
    if len(module_spec.parent["children"]) != 1:
        raise InvalidNexusStructureError(
            "Group containing f144 module should have exactly one child"
        )
    if module_spec.dtype is None or module_spec.value_units is None:
        raise InvalidNexusStructureError(
            "f144 module spec should have dtype and value_units"
        )


def _validate_ev44_module_spec(
    module_spec: StreamModuleValue,
) -> None:
    """Validate the ev44 module."""
    if len(module_spec.parent["children"]) != 1:
        raise InvalidNexusStructureError(
            "Group containing ev44 module should have exactly one child"
        )


FallBackNameType = TypeVar("FallBackNameType")


def _get_instrument_name(
    nxs_file: snx.File, fail_fall_back: FallBackNameType
) -> FallBackNameType:
    try:
        return nxs_file["entry/instrument/name"][()]
    except KeyError:
        return fail_fall_back


def _get_nx_class_attribute(group: snx.Group) -> dict[str, str]:
    if group.nx_class == snx.NXdetector:
        class_name = "NXdetector"
    elif group.nx_class == snx.NXmonitor:
        class_name = "NXmonitor"
    elif group.nx_class == snx.NXevent_data:
        class_name = "NXevent_data"
    else:
        raise NotImplementedError(f"NX class {group.nx_class} is not supported.")

    return {
        'name': 'NX_class',
        'values': class_name,
    }


def _group_to_dict(group: snx.Group) -> NexusGroup:
    return {
        "children": [],
        "name": group.name,
        "attributes": [_get_nx_class_attribute(group)],
    }


def _retrieve_groups_by_nx_class(
    nxs_file: snx.File, nx_class: type
) -> dict[tuple[str, ...], snx.Group]:
    instrument_path = "entry/instrument"
    return {
        tuple("/".join([instrument_path, grp_path]).split('/')): grp
        for grp_path, grp in nxs_file[instrument_path][nx_class].items()
    }


def _collect_nx_event(
    groups: dict[tuple[str, ...], snx.Group],
) -> dict[tuple[str, ...], snx.Group]:
    children = {}
    for path, group in groups.items():
        nx_events = group[snx.NXevent_data]
        for name, nx_event in nx_events.items():
            children[(*path, name)] = nx_event
    return children


def collect_streaming_modules_from_nexus_file(
    nexus_file: str | pathlib.Path,
) -> dict[StreamModuleKey, StreamModuleValue]:
    """Collect all stream modules in a nexus file."""

    with snx.File(nexus_file) as f:
        detectors = _collect_nx_event(_retrieve_groups_by_nx_class(f, snx.NXdetector))
        monitors = _collect_nx_event(_retrieve_groups_by_nx_class(f, snx.NXmonitor))
        instrument_name = _get_instrument_name(f, fail_fall_back="unknown")
        return {
            StreamModuleKey(
                module_type='ev44',
                topic=f"{instrument_name}_detector",
                source=det.parent.name,
            ): StreamModuleValue(
                path=det_path,
                parent=_group_to_dict(det),
            )
            for det_path, det in {**detectors, **monitors}.items()
        }


def collect_streaming_modules(
    structure: Mapping,
) -> dict[StreamModuleKey, StreamModuleValue]:
    """Collect all stream modules in a nexus template.

    Raises
    ------
    ValueError
        If the structure does not have a valid module place holder.
        - Contains ``topic`` and ``source`` in the configuration.
        - Every ``module`` - ``topic`` - ``source`` combination is unique.

    """
    # Collect stream module key-value pairs as tuple
    # to allow for validation of duplication before returning the dictionary.
    key_value_pairs = tuple(
        (
            StreamModuleKey(
                module_type=module_type,
                topic=config.get("topic", None),
                source=config.get("source", None),
            ),
            StreamModuleValue(
                # Modules do not have name so we remove the last element(None)
                path=(parent_path := path[:-1]),
                parent=cast(dict, find_nexus_structure(structure, parent_path)),
                dtype=config.get("dtype", config.get("type")),
                value_units=config.get("value_units", config.get("units")),
            ),
        )
        for path, node in iter_nexus_structure(structure)
        if
        (
            # Filter module place holders
            is_supported_module_type(module_type := node.get("module", ""))
            and isinstance((config := node.get("config")), dict)
        )
    )
    _validate_module_configs(key_value_pairs)
    _validate_module_keys(key_value_pairs)
    # Validate each spec
    for key, value in (key_value_dict := dict(key_value_pairs)).items():
        if key.module_type == 'ev44':
            _validate_ev44_module_spec(value)
        elif key.module_type == 'f144':
            _validate_f144_module_spec(value)
    return key_value_dict


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


def _is_monitor(group: NexusGroup) -> bool:
    return any(
        attr.get("name") == "NX_class" and attr.get("values") == "NXmonitor"
        for attr in group.get("attributes", ())
    )


def _initialize_ev44(module_spec: StreamModuleValue) -> NexusGroup:
    parent = module_spec.parent
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
    # TODO This check does not appear to work as expected.
    if not _is_monitor(group):
        # Monitor doesn't have pixel ids.
        group['children'].append(
            create_dataset(
                name="event_id",
                dtype="int32",
                initial_values=np.asarray([], dtype="int32"),
            )
        )
    return group


def _merge_ev44(group: NexusGroup, data: EventData) -> None:
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
        (event_time_zero_dataset["config"]["values"], data.reference_time)
    )
    event_time_offset_dataset = cast(
        NexusDataset, find_nexus_structure(group, ("event_time_offset",))
    )
    original_event_time_offset = event_time_offset_dataset["config"]["values"]
    event_time_offset_dataset["config"]["values"] = np.concatenate(
        (original_event_time_offset, data.time_of_flight)
    )
    # Increase event index according to the ``original_event_time_index``
    event_index_dataset = cast(
        NexusDataset, find_nexus_structure(group, ("event_index",))
    )
    event_index_dataset["config"]["values"] = np.concatenate(
        (
            event_index_dataset["config"]["values"],
            data.reference_time_index + len(original_event_time_offset),
        )
    )
    if data.pixel_id is not None:  # Pixel id is optional.
        event_id_dataset = cast(
            NexusDataset, find_nexus_structure(group, ("event_id",))
        )
        event_id_dataset["config"]["values"] = np.concatenate(
            (event_id_dataset["config"]["values"], data.pixel_id)
        )
    else:
        # TODO See above, monitor check is not working, remove now that we know there is
        # no pixel id.
        try:
            find_nexus_structure(group, ("event_id",))
        except KeyError:
            pass
        else:
            del group['children'][-1]


def _initialize_f144(module_spec: StreamModuleValue) -> NexusGroup:
    parent = module_spec.parent
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
            dtype=module_spec.dtype,
            initial_values=None,  # initial values should be replaced by the first data
            unit=module_spec.value_units,
        ),
    ]
    return group


def _merge_f144(group: NexusGroup, data: ExtractedLogData) -> None:
    time = cast(NexusDataset, find_nexus_structure(group, ("time",)))
    time["config"]["values"] = np.concatenate(
        (time["config"]["values"], [data.timestamp_unix_ns])
    )
    value = cast(NexusDataset, find_nexus_structure(group, ("value",)))
    if value["config"]["values"] is None:  # First data
        value["config"]["values"] = data.value
    else:
        value["config"]["values"] = np.concatenate(
            (value["config"]["values"], data.value)
        )


def _initialize_tdct(_: StreamModuleValue) -> NexusDataset:
    return create_dataset(
        name="top_dead_center",
        dtype="uint64",
        initial_values=np.asarray([], dtype="uint64"),
        unit="ns",
    )


def _merge_tdct(top_dead_center: NexusDataset, data: Timestamps) -> None:
    top_dead_center["config"]["values"] = np.concatenate(
        (top_dead_center["config"]["values"], data.timestamps)
    )


ModuleInitializer = Callable[[StreamModuleValue], Nexus]
MODULE_INITIALIZERS: Mapping[ModuleNameType, ModuleInitializer] = MappingProxyType(
    {
        "ev44": _initialize_ev44,
        "f144": _initialize_f144,
        "tdct": _initialize_tdct,
    }
)

ModuleMerger = (
    Callable[[Any, EventData], None]
    | Callable[[Any, Timestamps], None]
    | Callable[[Any, ExtractedLogData], None]
)
MODULE_MERGERS: Mapping[ModuleNameType, ModuleMerger] = MappingProxyType(
    {
        "ev44": _merge_ev44,
        "f144": _merge_f144,
        "tdct": _merge_tdct,
    }
)


def merge_message_into_nexus_store(
    *,
    module_key: StreamModuleKey,
    module_spec: StreamModuleValue,
    nexus_store: NexusStore,
    data: EventData | Timestamps | ExtractedLogData,
) -> None:
    """Merge a deserialized message into the associated nexus group or dataset."""
    if not is_supported_module_type(module_key.module_type):
        raise ValueError(f"Unsupported module type: {module_key.module_type}")

    initializer = MODULE_INITIALIZERS[module_key.module_type]
    merger = MODULE_MERGERS[module_key.module_type]
    if nexus_store.get(module_key) is None:
        nexus_store[module_key] = initializer(module_spec)
    merger(nexus_store[module_key], data)
