# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections import UserDict
from collections.abc import Hashable
from contextlib import contextmanager
from typing import TypeVar

from .data_subscriber import DataSubscriber

K = TypeVar('K', bound=Hashable)
V = TypeVar('V')


class DataService(UserDict[K, V]):
    """
    A service for managing and retrieving data and derived data.

    New data is set from upstream Kafka topics. Subscribers are typically plots that
    provide a live view of the data.
    """

    def __init__(self) -> None:
        super().__init__()
        self._subscribers: list[DataSubscriber[K]] = []
        self._pending_updates: set[K] = set()
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

    def register_subscriber(self, subscriber: DataSubscriber[K]) -> None:
        """
        Register a subscriber for updates.

        Parameters
        ----------
        subscriber:
            The subscriber to register. Must implement the DataSubscriber interface.
        """
        self._subscribers.append(subscriber)

    def _notify_subscribers(self, updated_keys: set[K]) -> None:
        """
        Notify relevant subscribers about data updates.

        Parameters
        ----------
        updated_keys
            The set of data keys that were updated.
        """
        for subscriber in self._subscribers:
            if updated_keys & subscriber.keys:
                # Pass only the data that the subscriber is interested in
                subscriber_data = {
                    key: self.data[key] for key in subscriber.keys if key in self.data
                }
                subscriber.trigger(subscriber_data)

    def __setitem__(self, key: K, value: V) -> None:
        super().__setitem__(key, value)
        self._pending_updates.add(key)

        # If not in transaction, immediately notify
        if not self._in_transaction:
            self._notify_subscribers({key})
            self._pending_updates.clear()
