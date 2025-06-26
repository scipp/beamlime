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


class StreamAssembler(ABC):
    """
    Base class for assembling data from a data store.

    This class defines the interface for assembling data from a data store based on
    specific keys. Subclasses must implement the `assemble` method.
    """

    def __init__(self, keys: set[DataKey]) -> None:
        """
        Initialize the assembler with its data dependencies.

        Parameters
        ----------
        keys:
            The set of data keys this assembler depends on. This is used to determine
            when the assembler will be triggered to assemble data, i.e., updates to
            which keys in :py:class:`DataService` will trigger the assembler to run.
        """
        self._keys = keys

    @property
    def keys(self) -> set[DataKey]:
        """Return the set of data keys this assembler depends on."""
        return self._keys

    @abstractmethod
    def assemble(self, data: dict[DataKey, Any]) -> Any:
        """
        Assemble data from the provided dictionary.

        Parameters
        ----------
        data:
            A dictionary containing data keyed by DataKey.

        Returns
        -------
        :
            The assembled data.
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

        Extracts the relevant data from the store based on the keys this pipe depends on
        and sends it to the pipe.

        Parameters
        ----------
        store:
            The complete data store containing all keys this pipe depends on. In
            practice this is the data dict of the :py:class:`DataService` instance.
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
            Data of any available keys the pipe depends on. May be a subset of the keys.
        """
