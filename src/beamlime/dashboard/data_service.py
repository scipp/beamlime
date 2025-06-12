# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections import UserDict
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import scipp as sc

from .data_key import ComponentDataKey, DataKey
from .pipe_base import PipeBase


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
        self._pipes: list[PipeBase] = []
        self._pending_updates: set[DataKey] = set()
        self._in_transaction = False

    def register_ui_pipe(self, pipe: PipeBase) -> None:
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
                pipe.trigger(self.data)

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
