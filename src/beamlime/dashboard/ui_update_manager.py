# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .data_service import DataKey, DataService


class UIPipe(ABC):
    """Base class for UI pipes that define their data dependencies."""

    @property
    @abstractmethod
    def keys(self) -> set[DataKey]:
        """Return the set of data keys this pipe depends on."""

    @abstractmethod
    def send(self, data: dict[DataKey, Any]) -> None:
        """
        Send data to the UI component.

        Parameters
        ----------
        data
            Complete data for all keys this pipe depends on.
        """


class UIUpdateManager:
    """Manages UI updates based on data changes."""

    def __init__(self, data_services: dict[str, DataService]) -> None:
        self._data_services = data_services
        self._pipes: dict[str, UIPipe] = {}

        # Register as listener for all data services
        for service in data_services.values():
            service.add_listener(self)

    def register_ui_pipe(self, pipe_name: str, pipe: UIPipe) -> None:
        """
        Register a UI pipe for updates.

        Parameters
        ----------
        pipe_name
            The name of the UI pipe.
        pipe
            The UI pipe instance that defines its own data dependencies.
        """
        self._pipes[pipe_name] = pipe

    def on_data_updated(self, keys: set[DataKey]) -> None:
        """
        Called when data has been updated.

        Parameters
        ----------
        keys
            The set of data keys that were updated.
        """
        # Check each pipe to see if any of its keys were updated
        for pipe_name, pipe in self._pipes.items():
            if keys & pipe.keys:  # intersection check
                self._send_ui_update(pipe_name, pipe)

    def _send_ui_update(self, pipe_name: str, pipe: UIPipe) -> None:
        """
        Send update to UI pipe with complete data for all its keys.

        Parameters
        ----------
        pipe_name
            The name of the pipe.
        pipe
            The pipe instance to send data to.
        """
        # Collect complete data for all keys the pipe needs
        complete_data = {}
        for key in pipe.keys:
            if service := self._data_services.get(key.service_name):
                if key in service:
                    complete_data[key] = service.get(key)

        pipe.send(complete_data)
