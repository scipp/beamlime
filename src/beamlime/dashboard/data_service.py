# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserDict
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

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


class UpdateListener(Protocol):
    """Protocol for objects that listen to data updates."""

    def on_data_updated(self, keys: set[DataKey]) -> None:
        """
        Called when data has been updated.

        Parameters
        ----------
        keys
            The set of data keys that were updated.
        """


DerivedGetter = Callable[['DataService', Hashable], Any | None]


class DataService(UserDict[DataKey, sc.DataArray]):
    """
    A service for managing and retrieving data and derivaed data.
    """

    def __init__(self) -> None:
        super().__init__()
        self._derived_getters: dict[Hashable, DerivedGetter] = {}
        self._listeners: list[UpdateListener] = []
        self._pending_updates: set[DataKey] = set()
        self._in_transaction = False

    def add_listener(self, listener: UpdateListener) -> None:
        """
        Add a listener for data updates.

        Parameters
        ----------
        listener
            The listener to add.
        """
        self._listeners.append(listener)

    def start_transaction(self) -> None:
        """Start a transaction to batch multiple updates."""
        self._in_transaction = True
        self._pending_updates.clear()

    def commit_transaction(self) -> None:
        """Commit the transaction and notify listeners of all updates."""
        if not self._in_transaction:
            return

        self._in_transaction = False
        if self._pending_updates:
            # Notify all listeners with the batch of updated keys
            for listener in self._listeners:
                listener.on_data_updated(self._pending_updates.copy())
            self._pending_updates.clear()

    def __setitem__(self, key: DataKey, value: sc.DataArray) -> None:
        super().__setitem__(key, value)
        self._pending_updates.add(key)

        # If not in transaction, immediately notify
        if not self._in_transaction:
            self.commit_transaction()


# TODO Weird, we don't want to register for all possible keys. Maybe this should just be
# a method after all?
def get_component_data(
    store: Mapping[DataKey, sc.DataArray], key: ComponentDataKey
) -> RawData | None:
    cumulative = store.get(key.cumulative_key(), None)
    current = store.get(key.current_key(), None)
    if cumulative is None or current is None:
        return None
    return RawData(cumulative=cumulative, current=current)


class TotalCountsGetter:
    def __init__(self, keys: Sequence[DataKey]) -> None:
        self.keys = keys

    def __call__(self, store: Mapping[DataKey, sc.DataArray]) -> dict[DataKey, int]:
        return {
            key: int(store[key].sum().value) if key in store else 0 for key in self.keys
        }


class MonitorDataService(DataService): ...


class DetectorDataService(DataService): ...


class ReducedDataService(DataService): ...


class DataForwarder:
    """A class to forward data to a data service based on stream names."""

    def __init__(self, data_services: dict[str, DataService]) -> None:
        self._data_services = data_services

    def __contains__(self, data_service_name: str) -> bool:
        """
        Check if a data service with the given name exists.

        Parameters
        ----------
        data_service_name:
            The name of the data service to check.

        Returns
        -------
        :
            True if the data service exists, False otherwise.
        """
        return data_service_name in self._data_services

    def start_transaction(self) -> None:
        """Start transactions across all data services."""
        for service in self._data_services.values():
            service.start_transaction()

    def commit_transaction(self) -> None:
        """Commit transactions across all data services."""
        for service in self._data_services.values():
            service.commit_transaction()

    def forward(self, stream_name: str, value: sc.DataArray) -> None:
        """
        Forward data to the appropriate data service based on the stream name.

        Parameters
        ----------
        stream_name:
            The name of the stream in the format 'source_name/service_name/suffix'. The
            suffix may contain additional '/' characters which will be ignored.
        value:
            The data to be forwarded.
        """
        try:
            source_name, service_name, key = stream_name.split('/', maxsplit=2)
        except ValueError:
            raise ValueError(
                f"Invalid stream name format '{stream_name}'. Expected format: "
                "'source_name/service_name/key'."
            ) from None
        if (service := self._data_services.get(service_name)) is not None:
            data_key = DataKey(
                service_name=service_name, source_name=source_name, key=key
            )
            service[data_key] = value
