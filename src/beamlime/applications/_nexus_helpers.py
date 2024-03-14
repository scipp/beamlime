# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os
from functools import partial
from types import MappingProxyType
from typing import Any, Callable, Literal, NamedTuple, Optional, Sequence, Union

import numpy as np

Path = Union[str, bytes, os.PathLike]


def _nested_mapping_proxy(obj: dict) -> MappingProxyType:
    """Create a nested shallow copy/mapping proxy of ``obj``.

    If any value in the dictionary is a dictionary,
    it is converted to a mapping proxy type.

    Examples
    --------
    >>> from types import MappingProxyType
    >>> _nested_mapping_proxy_type({'a': {'b': 1}})
    mappingproxy({'a': mappingproxy({'b': 1})})

    """
    return MappingProxyType(
        {
            k: _nested_mapping_proxy(v) if isinstance(v, dict) else v
            for k, v in obj.items()
        }
    )


class DatasetRecipe(NamedTuple):
    dtype: type
    ndim: int


FBIdentifier = Literal["ev44", "f144", "tdct"]
ValuesRecipe = MappingProxyType[str, type]
GROUP_RECIPES: MappingProxyType[FBIdentifier, ValuesRecipe] = _nested_mapping_proxy(
    {
        "f144": {"timestamp": DatasetRecipe(int, 0), "value": DatasetRecipe(float, 0)},
        "tdct": {"name": DatasetRecipe(str, 0), "timestamps": DatasetRecipe(int, 1)},
        "ev44": {
            "reference_time": DatasetRecipe(float, 1),
            "reference_time_index": DatasetRecipe(int, 1),
            "time_of_flight": DatasetRecipe(float, 1),
            "pixel_id": DatasetRecipe(int, 1),
        },
    }
)


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


class ModularDatasetContainer:
    """Modular dataset that can update/clear values."""

    def __init__(self, name: str, recipe: DatasetRecipe) -> None:
        self.recipe = recipe
        self.config_dict: dict[str, Any]
        self.config_dict = dict(name=name, dtype=recipe.dtype.__name__)
        self.dataset_dict = dict(module='dataset', config=self.config_dict)

    def update(self, new_values: Any) -> None:
        """Update the values from the ``other``.

        The values are extended if they are numpy.ndarray and replaced otherwise.
        """
        if 'values' not in self.config_dict or not isinstance(new_values, Sequence):
            self.config_dict['values'] = new_values
        elif isinstance((values := self.config_dict['values']), np.ndarray):
            self.config_dict['values'] = np.append(values, new_values)
        elif isinstance(values, list):
            values.extend(new_values)


class ModularDataGroupContainer:
    """Module container protocol."""

    def __init__(self, group_dict: dict, module_dict: dict, **initial_values) -> None:
        self.sub_datasets: dict[str, ModularDatasetContainer]
        self.module_dict = module_dict
        self.group_dict = group_dict
        self._populate_sub_dataset()
        self.update(initial_values)

    def _populate_sub_dataset(self) -> None:
        """Populate the sub datasets from the module dictionary."""
        module_id = self.module_dict['module']
        dataset_recipes = GROUP_RECIPES[module_id]
        self.group_dict['children'].remove(self.module_dict)
        self.sub_datasets = {
            name: ModularDatasetContainer(name, tp)
            for name, tp in dataset_recipes.items()
        }
        self.group_dict['children'].extend(self.sub_datasets.values())

    def update(self, other: dict) -> None:
        """Update the sub datasets from the other. Ignore if the name is not present.

        See :func:`~ModularDatasetContainer.update` for more details.
        """
        # If there is already a dataset with the same name, update it
        # or populate new ones otherwise
        for name, sub_dataset in self.sub_datasets.items():
            if name in other:
                sub_dataset.update(other[name])

    def clear(self) -> None:
        """Clear the sub datasets.

        See :func:`~ModularDatasetContainer.clear` for more details.
        """
        # Remove all datasets from the group
        ...


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
    nexus_template: dict

    @classmethod
    def from_template_file(cls, path: Path):
        import json

        with open(path) as f:
            return cls(nexus_template=json.load(f))

    def __init__(self, *, nexus_template: dict) -> None:
        instrument_gr = self._get_instrument_group(nexus_template)
        self.detectors = [
            NXDetectorContainer(det_dict)
            for det_dict in self._collect_detectors(instrument_gr)
        ]

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
