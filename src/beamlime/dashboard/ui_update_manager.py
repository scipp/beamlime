# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

from .data_service import DataKey, DataService


class UIUpdateManager:
    """Manages UI updates based on data changes."""

    def __init__(self, data_services: dict[str, DataService]) -> None:
        self._data_services = data_services
        self._ui_pipes: dict[str, Any] = {}  # panel.Pipe objects

        # Register as listener for all data services
        for service in data_services.values():
            service.add_listener(self)

    def register_ui_pipe(self, name: str, pipe: Any) -> None:
        """
        Register a Panel pipe for UI updates.

        Parameters
        ----------
        name:
            The name of the UI component.
        pipe:
            The Panel pipe object for sending updates.
        """
        self._ui_pipes[name] = pipe

    def on_data_updated(self, keys: set[DataKey]) -> None:
        """
        Called when data has been updated.

        Parameters
        ----------
        keys:
            The set of data keys that were updated.
        """
        # Group keys by service and determine what UI components need updates
        updates_by_service: dict[str, list[DataKey]] = {}
        for key in keys:
            if key.service_name not in updates_by_service:
                updates_by_service[key.service_name] = []
            updates_by_service[key.service_name].append(key)

        # Send targeted updates to relevant UI components
        for service_name, updated_keys in updates_by_service.items():
            self._send_ui_update(service_name, updated_keys)

    def _send_ui_update(self, service_name: str, keys: list[DataKey]) -> None:
        """
        Send update to UI for specific service.

        Parameters
        ----------
        service_name:
            The name of the service.
        keys:
            The list of data keys that were updated.
        """
        if pipe := self._ui_pipes.get(service_name):
            # Extract relevant data and send to UI
            service = self._data_services[service_name]
            update_data = {key: service.get(key) for key in keys if key in service}
            pipe.send(update_data)
