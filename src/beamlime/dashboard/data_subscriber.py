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


class DataSubscriber:
    """Unified subscriber that uses a StreamAssembler to process data."""

    def __init__(self, assembler: StreamAssembler, pipe: Pipe) -> None:
        """
        Initialize the subscriber with an assembler and pipe.

        Parameters
        ----------
        assembler:
            The assembler responsible for processing the data.
        pipe:
            The pipe to send assembled data to.
        """
        self._assembler = assembler
        self._pipe = pipe

    @property
    def keys(self) -> set[DataKey]:
        """Return the set of data keys this subscriber depends on."""
        return self._assembler.keys

    def trigger(self, store: dict[DataKey, Any]) -> None:
        """
        Trigger the subscriber with the current data store.

        Parameters
        ----------
        store:
            The complete data store containing all available data.
        """
        data = {key: store[key] for key in self.keys if key in store}
        if data:  # Only assemble and send if we have some data
            assembled_data = self._assembler.assemble(data)
            self._pipe.send(assembled_data)
