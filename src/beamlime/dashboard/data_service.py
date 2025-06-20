# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections import UserDict
from collections.abc import Callable, Hashable, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

import scipp as sc

from .data_key import DataKey
from .data_subscriber import DataSubscriber

DerivedGetter = Callable[['DataService', Hashable], Any | None]


class DataService(UserDict[DataKey, sc.DataArray]):
    """
    A service for managing and retrieving data and derivaed data.
    """

    def __init__(self) -> None:
        super().__init__()
        self._derived_getters: dict[Hashable, DerivedGetter] = {}
        self._subscribers: list[DataSubscriber] = []
        self._pending_updates: set[DataKey] = set()
        self._transaction_depth = 0

    @contextmanager
    def transaction(self):
        """Context manager for batching multiple updates."""
        self._transaction_depth += 1
        try:
            yield
        finally:
            self._transaction_depth -= 1
            if self._transaction_depth == 0 and self._pending_updates:
                self._notify_subscribers(self._pending_updates)
                self._pending_updates.clear()

    @property
    def _in_transaction(self) -> bool:
        return self._transaction_depth > 0

    def register_subscriber(self, subscriber: DataSubscriber) -> None:
        """
        Register a subscriber for updates.

        Parameters
        ----------
        subscriber:
            The subscriber to register. Must implement the DataSubscriber interface.
        """
        self._subscribers.append(subscriber)

    def _notify_subscribers(self, updated_keys: set[DataKey]) -> None:
        """
        Notify relevant subscribers about data updates.

        Parameters
        ----------
        updated_keys
            The set of data keys that were updated.
        """
        for subscriber in self._subscribers:
            if updated_keys & subscriber.keys:
                subscriber.trigger(self.data)

    def __setitem__(self, key: DataKey, value: sc.DataArray) -> None:
        super().__setitem__(key, value)
        self._pending_updates.add(key)

        # If not in transaction, immediately notify
        if not self._in_transaction:
            self._notify_subscribers({key})
            self._pending_updates.clear()


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
