# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserDict
from collections.abc import Callable, Hashable, Mapping
from dataclasses import dataclass
from typing import Any

import scipp as sc


@dataclass(frozen=True, slots=True, kw_only=True)
class RawData:
    cumulative: sc.DataArray
    current: sc.DataArray


@dataclass(frozen=True, slots=True, kw_only=True)
class DataKey:
    service_name: str
    source_name: str
    key: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ComponentDataKey(ABC):
    component_name: str
    view_name: str

    @property
    @abstractmethod
    def service_name(self) -> str:
        """
        Returns the name of the service this component belongs to.
        This is used to identify the service in the data store.
        """

    def cumulative_key(self) -> DataKey:
        return DataKey(
            service_name=self.service_name,
            source_name=self.component_name,
            key=f'{self.view_name}/cumulative',
        )

    def current_key(self) -> DataKey:
        return DataKey(
            service_name=self.service_name,
            source_name=self.component_name,
            key=f'{self.view_name}/current',
        )


class MonitorDataKey(ComponentDataKey):
    @property
    def service_name(self) -> str:
        return 'monitor_data'


class DetectorDataKey(ComponentDataKey):
    @property
    def service_name(self) -> str:
        return 'detector_data'


DerivedGetter = Callable[['DataService', Hashable], Any | None]


class DataService(UserDict[DataKey, sc.DataArray]):
    """
    A service for managing and retrieving data and derivaed data.
    """

    def __init__(self) -> None:
        super().__init__()
        self._derived_getters: dict[Hashable, DerivedGetter] = {}

    def register_derived_getter(self, key: Hashable, getter: DerivedGetter) -> None:
        """
        Register a derived getter for a specific data key.

        Parameters
        ----------
        key:
            The data key for which the derived getter is registered.
        getter:
            A callable that takes a DataService and a DataKey and returns a DataArray.
        """
        self._derived_getters[key] = getter

    def get_derived(self, key: Hashable) -> Any | None:
        """
        Get a derived value for a specific data key.

        Parameters
        ----------
        key:
            The data key for which the derived value is requested.

        Returns
        -------
        The derived value.
        """
        if (getter := self._derived_getters.get(key)) is not None:
            return getter(self, key)
        if isinstance(key, DataKey):
            # If the key is a DataKey, we can try to get the data directly
            return self.data.get(key, None)
        raise KeyError(f"No derived getter registered for key: {key}")


def get_detector_data(
    store: Mapping[DataKey, sc.DataArray], key: DetectorDataKey
) -> RawData | None:
    cumulative = store.get(key.cumulative_key(), None)
    current = store.get(key.current_key(), None)
    if cumulative is None or current is None:
        return None
    return RawData(cumulative=cumulative, current=current)
