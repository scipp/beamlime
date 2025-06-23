# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol

from .data_service import DataKey


class Pipe(Protocol):
    """
    Protocol for downstream pipes that can receive data from upstream pipes.
    """

    def send(self, data: Any) -> None:
        """
        Send data to the downstream pipe.

        Parameters
        ----------
        data:
            The data to be sent.
        """


class DataSubscriber(ABC):
    """Base class for pipes that define their data dependencies."""

    def __init__(self, keys: set[DataKey]) -> None:
        """
        Initialize the pipe with its data dependencies.

        Parameters
        ----------
        keys:
            The set of data keys this pipe depends on.
        """
        self._keys = keys

    @property
    def keys(self) -> set[DataKey]:
        """Return the set of data keys this pipe depends on."""
        return self._keys

    def trigger(self, store: dict[DataKey, Any]) -> None:
        """
        Trigger the pipe with the current data store.

        Parameters
        ----------
        store:
            The complete data store containing all keys this pipe depends on.
        """
        data = {key: store[key] for key in self.keys if key in store}
        self.send(data)

    @abstractmethod
    def send(self, data: dict[DataKey, Any]) -> None:
        """
        Send data. Must be implemented by subclasses.

        Parameters
        ----------
        data:
            Complete data for all keys this pipe depends on.
        """
