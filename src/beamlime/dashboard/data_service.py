# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections import UserDict
from collections.abc import Callable
from dataclasses import dataclass

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
class DetectorDataKey:
    detector_name: str
    view_name: str

    def cumulative_key(self) -> DataKey:
        return DataKey(
            service_name='detector_data',
            source_name=self.detector_name,
            key=f'{self.view_name}/cumulative',
        )

    def current_key(self) -> DataKey:
        return DataKey(
            service_name='detector_data',
            source_name=self.detector_name,
            key=f'{self.view_name}/current',
        )


DerivedGetter = Callable[['DataService', DataKey], sc.DataArray]


class DataService(UserDict):
    """
    A service for managing and retrieving data and derivaed data.
    """

    def __init__(self) -> None:
        super().__init__()
        self._derived_getters: dict[DataKey, DerivedGetter] = {}

    def __getitem__(self, key: DataKey) -> sc.DataArray | sc.DataGroup:
        """
        Retrieve a DataArray for the given key.

        If a derived getter is registered for the key, it will be used to compute the
        output. Otherwise, it will retrieve the data from the underlying dictionary.

        Parameters
        ----------
        key:
            The data key to retrieve.

        Returns
        -------
        :
            The data associated with the key.
        """
        if key in self._derived_getters:
            return self._derived_getters[key](self, key)
        return super().__getitem__(key)

    def get_detector_data(self, key: DetectorDataKey) -> RawData | None:
        cumulative = self.get(key.cumulative_key(), None)
        current = self.get(key.current_key(), None)
        if cumulative is None or current is None:
            return None
        return RawData(cumulative=cumulative, current=current)

    def register_derived_getter(self, key: DataKey, getter: DerivedGetter) -> None:
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
