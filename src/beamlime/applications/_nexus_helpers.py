# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os
from functools import partial
from typing import (
    Any,
    Callable,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

Path = Union[str, bytes, os.PathLike]
DTypeDesc = Literal["int", "float", "string"]


class DatasetRecipe(NamedTuple):
    name: str
    dtype_description: DTypeDesc


FlatBufferIdentifier = Literal["ev44", "f144", "tdct"]
GROUP_RECIPES: Mapping[FlatBufferIdentifier, Tuple[DatasetRecipe, ...]] = {
    "f144": (DatasetRecipe("timestamp", "int"), DatasetRecipe("value", "float")),
    "tdct": (DatasetRecipe("name", "string"), DatasetRecipe("timestamps", "int")),
    "ev44": (
        DatasetRecipe("reference_time", "int"),  # In nanoseconds
        DatasetRecipe("reference_time_index", "int"),
        DatasetRecipe("time_of_flight", "int"),  # In nanoseconds
        DatasetRecipe("pixel_id", "int"),
    ),
}


T = TypeVar("T")


NestedObjectT = TypeVar("NestedObjectT", dict, list[dict])


def nested_shallow_copy(obj: NestedObjectT, *indices) -> NestedObjectT:
    """Shallow copy the nested dictionary and lists.

    Examples
    --------
    >>> a = {'a': 1, 'b': {'b-a': 2}, 'c': {'c-a': 3}}
    >>> b = nested_shallow_copy(a, 'b')
    >>> b['b']['b-a'] = 3
    >>> a['b']['b-a']  # The original dictionary is not changed
    2
    >>> b['b']['b-a']  # The copied dictionary is changed
    3
    >>> b['c']['c-a'] = 4
    >>> a['c']['c-a']  # The original dictionary is also changed
    4
    """
    from copy import copy

    root_obj = obj.copy()
    cur_parent = root_obj

    try:
        for idx in indices:
            cur_parent[idx] = copy(cur_parent[idx])
            cur_parent = cur_parent[idx]
    except (KeyError, TypeError) as e:
        raise KeyError(f"Invalid path: {indices}") from e

    return root_obj


def find_index(
    obj: dict | list[dict],
    *filter_or_indices: Callable | str | int,
    _cur_idx: Optional[tuple[str | int, ...]] = None,
) -> tuple[str | int, ...]:
    """Find the index of the object in the nested dictionary."""
    cur_idx = _cur_idx or tuple()

    if not filter_or_indices:
        return cur_idx

    cur_filter, _left_filters = filter_or_indices[0], filter_or_indices[1:]
    if (isinstance(obj, dict) and isinstance(cur_filter, str)) or (
        isinstance(obj, list) and isinstance(cur_filter, int)
    ):
        return find_index(
            obj[cur_filter], *_left_filters, _cur_idx=cur_idx + (cur_filter,)
        )
    elif isinstance(obj, dict) and callable(cur_filter):
        key, value = next((k, v) for k, v in obj.items() if cur_filter(k, v))
        return find_index(value, *_left_filters, _cur_idx=cur_idx + (key,))
    elif isinstance(obj, list) and callable(cur_filter):
        idx, value = next(((i, v) for i, v in enumerate(obj) if cur_filter(v)))
        return find_index(value, *_left_filters, _cur_idx=cur_idx + (idx,))
    else:
        raise KeyError(
            f"Invalid filter function or index: {cur_filter} for type: {type(obj)}"
        )


def nested_dict_getitem(obj: dict, *indexes: str | int) -> Any:
    """Get the item from the nested dictionary."""
    if not indexes:
        return obj
    return nested_dict_getitem(obj[indexes[0]], *indexes[1:])


def nested_dict_select(
    obj: dict, *filter_or_indices: Callable[..., bool] | str | int
) -> Any:
    """Select the first matching item from the nested dictionary."""
    return nested_dict_getitem(obj, *find_index(obj, *filter_or_indices))


def match_nx_class(child: dict, cls_name: str) -> bool:
    attrs = child.get('attributes', [])
    return any(
        attr.get('name', None) == "NX_class" and attr.get("values", None) == cls_name
        for attr in attrs
    )


def _match_dataset_name(child: dict, name: str) -> bool:
    return (
        child.get('module', None) == 'dataset'
        and child.get('config', {}).get('name', None) == name
    )


def populate_dataset(dataset_recipe: DatasetRecipe, initial_values: Any = None) -> dict:
    """Create a dataset dictionary."""

    return dict(
        module='dataset',
        config=dict(
            name=dataset_recipe.name,
            dtype=dataset_recipe.dtype_description,
            values=initial_values,
        ),
    )


class ModularDataGroupRecipe:
    """Modular data group information."""

    def __init__(
        self,
        root_dict: dict,
        path_to_module: tuple[int | str, ...],
    ) -> None:
        """Initialize the modular group container.

        Parameters
        ----------
        root_dict:
            The dictionary of the whole structure.

        path_to_module:
            The path to the module placeholder in the nexus template from the root.

        """
        self.target_path = path_to_module[:-1]  # Up to (..., "children")
        self.module_dict = nested_dict_getitem(root_dict, *path_to_module)
        self.module_id = self.module_dict['module']
        self.sub_dataset_recipes = GROUP_RECIPES[self.module_id]


def replace_subdataset(
    root: dict, modular_group_container: ModularDataGroupRecipe, other: Mapping
) -> dict:
    """Make a shallow copy or ``root`` and update it from ``other``.

    Path to the sub-dataset is provided by the ``modular_group_container``.
    Ignore if the name is not present in a recipe.
    """
    copied_root = nested_shallow_copy(root, *modular_group_container.target_path)
    sub_dataset_list: list[dict] = nested_dict_select(
        copied_root,
        *modular_group_container.target_path,
    )
    sub_dataset_list[:] = [
        populate_dataset(dataset_recipe, initial_values=other[dataset_recipe.name])
        for dataset_recipe in modular_group_container.sub_dataset_recipes
        if dataset_recipe.name in other
    ]
    return copied_root


def _match_module_name(child: dict, name: str) -> bool:
    return child.get('module', None) == name


def collect_nested_module_indices(
    nexus_container: dict, fb_id: FlatBufferIdentifier
) -> list[tuple]:
    """Collect the indices of the module with the given id."""
    collected = list()

    def _recursive_collect_module_indices(
        obj: dict,
        cur_idx: tuple[str | int, ...],
    ) -> None:
        # Find module placeholder in the group.
        for i, cand in enumerate(
            [child for child in obj.get('children', []) if isinstance(child, dict)]
        ):
            cand_idx = cur_idx + ('children', i)
            if _match_module_name(cand, fb_id):
                collected.append(cand_idx)
            else:
                _recursive_collect_module_indices(cand, cand_idx)

    _recursive_collect_module_indices(nexus_container, tuple())
    return collected


def check_multi_module_datagroup(nexus_template: dict) -> None:
    """Check if the template has multiple modules in single data group."""
    module_indices = []
    for fb_id in GROUP_RECIPES.keys():
        module_indices.extend(collect_nested_module_indices(nexus_template, fb_id))

    parent_indices = [idx[:-1] for idx in module_indices]
    if len(parent_indices) > len(set(parent_indices)):
        raise ValueError("Multiple modules found in the same data group.")


class NXDetectorContainer:
    """NXDetector container.

    This container is only for fake event data feeding.
    """

    def __init__(self, detector_dict: dict) -> None:
        self.pixel_ids: list[int]
        self.pixel_offsets: dict[str, list[float]]

        self.detector_name: str = detector_dict['name']
        self.detector_dict = detector_dict

        self.init_pixel_ids()

    def init_pixel_ids(self) -> None:
        """Initialize the pixel ids of the detector."""
        pixel_id_filter = partial(_match_dataset_name, name='detector_number')
        self._pixel_ids_idx = find_index(
            self.detector_dict, 'children', pixel_id_filter, 'config', 'values'
        )
        self.pixel_ids = nested_dict_getitem(self.detector_dict, *self._pixel_ids_idx)


class NexusContainer:
    @classmethod
    def from_template_file(cls, path: Path):
        import json

        with open(path) as f:
            return cls(nexus_template=json.load(f))

    @property
    def nexus_dict(self):
        return self._nexus_dict

    def __init__(self, *, nexus_template: dict) -> None:
        # Validate the nexus template
        check_multi_module_datagroup(nexus_template)
        self._nexus_dict = nexus_template
        instrument_gr = self._get_instrument_group(nexus_template)
        self.detectors = {
            det.detector_name: det
            for det in [
                NXDetectorContainer(det_dict)
                for det_dict in self._collect_detectors(instrument_gr)
            ]
        }
        self.modules: dict[str, dict[str, ModularDataGroupRecipe]] = dict()
        self.modules['ev44'] = self._collect_ev44()

    def _collect_detectors(self, instrument_gr: dict) -> list[dict]:
        """Collect the detectors from the instrument group.

        One or more NXdetectors are expected per NXinstrument.
        """
        return [
            child
            for child in instrument_gr['children']
            if match_nx_class(child, 'NXdetector')
            # Expecting one or more NXdetector
        ]

    def _get_instrument_group(self, nexus_container: dict) -> dict:
        """Get the NXinstrument group from the nexus container.

        Only one NXinstrument group is expected in one template.
        """
        return nested_dict_select(
            nexus_container,
            'children',
            partial(match_nx_class, cls_name='NXentry'),
            'children',
            partial(match_nx_class, cls_name='NXinstrument'),
        )

    def _collect_ev44(self) -> dict[str, ModularDataGroupRecipe]:
        """Collect the ev44 modules from the detector or monitor groups."""

        def _retrieve_key(module_dict: dict) -> str:
            return module_dict['config']['source']

        module_recipes = [
            ModularDataGroupRecipe(self._nexus_dict, idx)
            for idx in collect_nested_module_indices(self._nexus_dict, 'ev44')
        ]
        return {
            _retrieve_key(module_recipe.module_dict): module_recipe
            for module_recipe in module_recipes
        }

    def insert_ev44(self, ev44_data: Mapping) -> None:
        """Insert the module data."""
        module = self.modules['ev44'][ev44_data['source_name']]
        updated = replace_subdataset(
            root=self._nexus_dict, modular_group_container=module, other=ev44_data
        )
        self._nexus_dict = updated
