# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Iterable,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
)

import numpy as np

NexusDtype = Literal["int", "float", "string"]
"""Supported data types in the nexus json format to be parsed by ``scippnexus``."""


class _NexusDataset(TypedDict):
    """``dataset`` module structure in the nexus json format."""

    module: str
    config: dict[str, Any]
    attributes: list[Mapping[str, Any]]


@dataclass(frozen=True)
class DatasetRecipe:
    """Recipe that describes dtype and unit of a dataset in a module."""

    name: str
    """Real name of the dataset in the file."""
    dtype: NexusDtype
    """Data type of the dataset in string."""
    unit: str | None = None
    """Unit of the dataset in string."""

    def populate(self, values: Any) -> _NexusDataset:
        """Populates the dataset according to the recipe."""
        dataset: _NexusDataset = {
            "module": "dataset",
            "config": {
                "name": self.name,
                "dtype": self.dtype,
                "values": values,
            },
            "attributes": [],
        }
        if self.unit is not None:
            attrs = dataset["attributes"]
            attrs.append(
                {
                    "name": "units",
                    "dtype": "string",
                    "values": self.unit,
                }
            )
        return dataset


FlatBufferSchemaFieldNameType = Literal[
    # ev44
    "reference_time",
    "reference_time_index",
    "time_of_flight",
    "pixel_id",
]
ModuleRecipe = Mapping[FlatBufferSchemaFieldNameType, DatasetRecipe]
"""Module recipe that describes the dataset structure of a module.

For example,
{
    "event_id": DatasetRecipe(dtype="int64"),
    "event_time_offset": DatasetRecipe(dtype="float", unit="ns"),
}
"""

ModuleNameType = Literal["ev44"]  # "f144", "tdct" will be added in the future
"""Name of the module that is supported by beamlime."""

_EV44_RECIPE: ModuleRecipe = MappingProxyType(
    {
        "reference_time": DatasetRecipe(name="event_time_zero", dtype="int", unit="ns"),
        "reference_time_index": DatasetRecipe(name="event_index", dtype="int"),
        "time_of_flight": DatasetRecipe(
            name="event_time_offset", dtype="int", unit="ns"
        ),
        "pixel_id": DatasetRecipe(name="event_id", dtype="int"),
    }
)
MODULE_RECIPE_MAPS: Mapping[ModuleNameType, ModuleRecipe] = MappingProxyType(
    {
        "ev44": _EV44_RECIPE,
    }
)
"""Default dataset recipes for the supported modules."""


def _merge_ev44(group: dict, data_piece: Mapping) -> None:
    for field, value in data_piece.items():
        if value is None or field not in _EV44_RECIPE:
            # Could be monitor without pixel_id
            # or other fields that is not included in the file.
            continue
        try:
            dataset = find_nexus_structure(group, (_EV44_RECIPE[field].name,))
            if _EV44_RECIPE[field].name in (
                "event_time_zero",
                "event_index",
                "event_time_offset",
                "pixel_id",
            ):
                dataset["config"]["values"] = np.concatenate(
                    (dataset["config"]["values"], value)
                )
            else:
                dataset["config"]["values"] = value
        except KeyError:
            group.setdefault('children', []).append(_EV44_RECIPE[field].populate(value))


def _node_name(n):
    """Defines the name of a nexus tree branch or leaf"""
    config = n.get("config", {})
    return n.get("name", config.get("name"))


def iter_nexus_structure(
    structure: Mapping, root: Optional[Tuple[Optional[str], ...]] = None
) -> Iterable[tuple[tuple[str | None, ...], Mapping]]:
    """Visits all branches and leafs in the nexus tree"""
    path = (*root, _node_name(structure)) if root is not None else tuple()
    yield path, structure
    for child in structure.get("children", []):
        yield from iter_nexus_structure(child, root=path)


def find_nexus_structure(structure: Mapping, path: Sequence[Optional[str]]) -> Mapping:
    """Returns the branch or leaf associated with `path`, or None if not found"""
    if len(path) == 0:
        return structure
    head, *tail = path
    for child in structure["children"]:
        if head == _node_name(child):
            return find_nexus_structure(child, tail)
    raise KeyError(f"Path {path} not found in the nexus structure.")


def find_ev44_matching_paths(
    structure: Mapping, data_piece: Mapping
) -> Iterable[Tuple[Optional[str], ...]]:
    source_name = data_piece["source_name"]
    for path, node in iter_nexus_structure(structure):
        if (
            node.get("module") == "ev44"
            and node.get("config", {}).get("source") == source_name
        ):
            yield path


def _merge_message_into_store(
    *,
    store: dict[tuple[str | None, ...], dict],
    structure: Mapping,
    data_piece: Mapping,
    path_matching_func: Callable[[Mapping, Mapping], Iterable[tuple[str | None, ...]]],
    merge_func: Callable[[dict, Mapping], None],
) -> None:
    """Template function to merge a message into the store.

    This template-style function was needed since different modules
    have different ways to match the paths and merge the data.

    Parameters
    ----------
    store:
        The store that holds the data.
    structure:
        The nexus structure.
    data_piece:
        The content of the message.
    path_matching_func:
        A function that returns the paths that match the message.
    merge_func:
        A function that merges the message into the store.

    Side Effects
    ------------
    The ``store`` is updated with the merged data.

    """
    for path in path_matching_func(structure, data_piece):
        if path not in store:
            parent = find_nexus_structure(structure, path[:-1])
            store[path] = dict(
                children=[],
                name=_node_name(parent),
            )
            if "attributes" in parent:
                store[path]["attributes"] = parent["attributes"]
            if len(parent["children"]) > 1:
                raise ValueError("Multiple modules found in the same data group.")
        merge_func(store[path], data_piece)


def merge_message_into_store(
    store: dict[tuple[str | None, ...], dict],
    structure: Mapping,
    kind: ModuleNameType,
    data_piece: Mapping,
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
    if kind == "ev44":
        _merge_message_into_store(
            store=store,
            structure=structure,
            data_piece=data_piece,
            path_matching_func=find_ev44_matching_paths,
            merge_func=_merge_ev44,
        )
    else:
        raise NotImplementedError


def combine_store_and_structure(
    store: Mapping[Tuple[Optional[str], ...], Mapping], structure: dict
):
    """Creates a new nexus structure, replacing the stream modules
    with the datasets in `store`, while avoiding
    to copy data from `structure` if unnecessary"""
    if len(store) == 0:
        return structure
    if (None,) in store:
        return store[(None,)]

    new = structure.copy()
    if "children" in structure:
        children = []
        for child in structure["children"]:
            children.append(
                combine_store_and_structure(
                    {
                        tuple(tail): group
                        for (head, *tail), group in store.items()
                        if head == _node_name(child)
                    },
                    child,
                )
            )
        new["children"] = children
    return new
