# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os
from functools import partial
from types import MappingProxyType
from typing import Any, Callable, Literal, Optional, Union

Path = Union[str, bytes, os.PathLike]


FBIdentifier = Literal["ev44", "f144", "tdct"]
ValuesRecipe = MappingProxyType[str, type]

_EV44_RECIPE = MappingProxyType(
    {
        "reference_time": list[float],
        "reference_time_index": list[int],
        "time_of_flight": list[float],
        "pixel_id": list[int],
    }
)


_F144_RECIPE = MappingProxyType({"timestamp": int, "value": float})


_TDCT_RECIPE = MappingProxyType({"name": str, "timestamps": list[int]})


MODULE_RECIPES: MappingProxyType[FBIdentifier, ValuesRecipe] = MappingProxyType(
    {
        "ev44": _EV44_RECIPE,
        "f144": _F144_RECIPE,
        "tdct": _TDCT_RECIPE,
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


def _match_module_name(child: dict, name: str) -> bool:
    return child.get('module', None) == name


def collect_nested_module_indices(
    nexus_container: dict, fb_id: FBIdentifier
) -> list[tuple]:
    """Collect the indices of the module with the given id."""
    collected = list()

    def _recursive_collect_indices(
        obj: dict,
        cur_idx: tuple[str | int, ...],
    ) -> None:
        # Group
        for i, cand in enumerate(obj.get('children', [])):
            cand_idx = cur_idx + ('children', i)
            if _match_module_name(cand, fb_id):
                collected.append(cand_idx)
            else:
                _recursive_collect_indices(cand, cand_idx)

    _recursive_collect_indices(nexus_container, tuple())
    return collected


class ModularDatasetContainer(dict):
    """Modular dataset that can update/clear values."""

    def __init__(self, name: str, tp: type) -> None:
        self.config_container = dict(name=name, dtype=tp.__name__)
        super().__init__(module='dataset', config=self.config_container)

    def clear(self) -> None:
        """Clear the values if they are list. Ignore otherwise."""
        if 'values' in self.config_container and isinstance(
            self.config_container['values'], list
        ):
            self.config_container['values'].clear()
        elif 'values' in self.config_container:
            del self.config_container['values']

    def update(self, values: Any) -> None:
        """Update the values from the ``other``.

        The values are extended if they are list and replaced otherwise.
        """
        if 'values' not in self.config_container or not isinstance(values, list):
            self.config_container['values'] = values
        elif isinstance(self.config_container['values'], list):
            self.config_container['values'].extend(values)


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
        dataset_recipes = MODULE_RECIPES[module_id]
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
        for name, sub_dataset in self.sub_datasets.items():
            if name in other:
                sub_dataset.update(other[name])

    def clear(self) -> None:
        """Clear the sub datasets.

        See :func:`~ModularDatasetContainer.clear` for more details.
        """
        for sub_dataset in self.sub_datasets.values():
            sub_dataset.clear()


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

        Only one or more NXdetectors are expected.
        """
        return [
            child
            for child in instrument_gr['children']
            if match_nx_class(child, 'NXdetector')
            # Expecting one or more NXdetector
        ]

    def _get_instrument_group(self, nexus_container: dict) -> dict:
        """Get the NXinstrument group from the nexus container.

        Only one NXinstrument group is expected.
        """
        return nested_dict_select(
            nexus_container,
            'children',
            partial(match_nx_class, cls_name='NXentry'),
            'children',
            partial(match_nx_class, cls_name='NXinstrument'),
        )
