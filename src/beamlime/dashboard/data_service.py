# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections import UserDict
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import scipp as sc

from .data_key import ComponentDataKey, DataKey
from .multi_pipe import MultiPipe


@dataclass(frozen=True, slots=True, kw_only=True)
class RawData:
    cumulative: sc.DataArray
    current: sc.DataArray


DerivedGetter = Callable[['DataService', Hashable], Any | None]


class DataService(UserDict[DataKey, sc.DataArray]):
    """
    A service for managing and retrieving data and derivaed data.
    """

    def __init__(self) -> None:
        super().__init__()
        self._derived_getters: dict[Hashable, DerivedGetter] = {}
        self._pipes: list[MultiPipe] = []
        self._pending_updates: set[DataKey] = set()
        self._in_transaction = False

    def register_ui_pipe(self, pipe: MultiPipe) -> None:
        """
        Register a pipe for updates.

        Parameters
        ----------
        pipe
            The pipe instance that defines its own data dependencies.
        """
        self._pipes.append(pipe)

    def start_transaction(self) -> None:
        """Start a transaction to batch multiple updates."""
        self._in_transaction = True
        self._pending_updates.clear()

    def commit_transaction(self) -> None:
        """Commit the transaction and notify pipes of all updates."""
        if not self._in_transaction:
            return

        self._in_transaction = False
        if self._pending_updates:
            self._notify_pipes(self._pending_updates)
            self._pending_updates.clear()

    def _notify_pipes(self, updated_keys: set[DataKey]) -> None:
        """
        Notify relevant pipes about data updates.

        Parameters
        ----------
        updated_keys
            The set of data keys that were updated.
        """
        for pipe in self._pipes:
            if updated_keys & pipe.keys:
                self._send_pipe_update(pipe)

    def _send_pipe_update(self, pipe: MultiPipe) -> None:
        """
        Send update to UI pipe with complete data for all its keys.

        Parameters
        ----------
        pipe
            The pipe instance to send data to.
        """
        complete_data = {}
        for key in pipe.keys:
            if key.service_name == self._get_service_name() and key in self:
                complete_data[key] = self.get(key)
        pipe.send(complete_data)

    def _get_service_name(self) -> str:
        """Get the service name for this data service."""
        class_name = self.__class__.__name__
        if class_name.endswith('DataService'):
            return class_name[:-11].lower() + '_data'
        return class_name.lower()

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
