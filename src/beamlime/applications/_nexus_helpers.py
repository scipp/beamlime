# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import os
from functools import partial
from typing import Any, Callable, Literal, Optional, Union

Path = Union[str, bytes, os.PathLike]
FlatBufferIdentifier = Literal["ev44", "f144", "tdct"]


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
