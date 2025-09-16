# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections import UserDict
from collections.abc import Hashable
from contextlib import contextmanager
from typing import TypeVar, Callable

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
        self._key_change_subscribers: list[Callable[[set[K], set[K]], None]] = []
        self._pending_updates: set[K] = set()
        self._pending_key_additions: set[K] = set()
        self._pending_key_removals: set[K] = set()
        self._transaction_depth = 0

    @contextmanager
    def transaction(self):
        """Context manager for batching multiple updates."""
        self._transaction_depth += 1
        try:
            yield
        finally:
            # Stay in transaction until notifications are done
            if self._transaction_depth == 1:
                # Some updates may have been added while notifying
                while self._pending_updates:
                    pending = set(self._pending_updates)
                    self._pending_updates.clear()
                    self._notify_subscribers(pending)
                    self._notify_key_change_subscribers()
                    self._pending_key_additions.clear()
                    self._pending_key_removals.clear()
            self._transaction_depth -= 1

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

    def subscribe_to_changed_keys(
        self, subscriber: Callable[[set[K], set[K]], None]
    ) -> None:
        """
        Register a subscriber for key change updates (additions/removals).

        Parameters
        ----------
        subscriber:
            A callable that accepts two sets: added_keys and removed_keys.
        """
        self._key_change_subscribers.append(subscriber)
        subscriber(set(self.data.keys()), set())

    def _notify_subscribers(self, updated_keys: set[K]) -> None:
        """
        Notify relevant subscribers about data updates.

        Parameters
        ----------
        updated_keys
            The set of data keys that were updated.
        """
        for subscriber in self._subscribers:
            if not isinstance(subscriber, DataSubscriber):
                subscriber(updated_keys)
                continue
            if updated_keys & subscriber.keys:
                # Pass only the data that the subscriber is interested in
                subscriber_data = {
                    key: self.data[key] for key in subscriber.keys if key in self.data
                }
                subscriber.trigger(subscriber_data)

    def _notify_key_change_subscribers(self) -> None:
        """Notify subscribers about key changes (additions/removals)."""
        if not self._pending_key_additions and not self._pending_key_removals:
            return

        for subscriber in self._key_change_subscribers:
            subscriber(
                self._pending_key_additions.copy(), self._pending_key_removals.copy()
            )

    def __setitem__(self, key: K, value: V) -> None:
        is_new_key = key not in self.data
        super().__setitem__(key, value)
        self._pending_updates.add(key)
        if is_new_key:
            self._pending_key_additions.add(key)
        self._notify_if_not_in_transaction(key)

    def __delitem__(self, key: K) -> None:
        super().__delitem__(key)
        self._pending_updates.add(key)
        self._pending_key_removals.add(key)
        self._notify_if_not_in_transaction(key)

    def _notify_if_not_in_transaction(self, key: K) -> None:
        """Notify subscribers if not in a transaction."""
        if not self._in_transaction:
            self._notify_subscribers({key, *self._pending_updates})
            self._notify_key_change_subscribers()
            self._pending_updates.clear()
            self._pending_key_additions.clear()
            self._pending_key_removals.clear()
