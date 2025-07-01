# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import panel as pn

from beamlime.dashboard.controller_factory import Controller


class ConfigWidget(ABC):
    """
    Abstract base class for configuring a single config value.

    As config values are defined as pydantic models, each widget can be composed of
    sub-widgets. This base class automatically sets up two-way linking between the
    widget and the configuration service via the controller.
    """

    def __init__(self, controller: Controller) -> None:
        """
        Initialize the config widget with a controller.

        Parameters
        ----------
        controller
            Controller for managing the configuration value.
        """
        self._controller = controller
        self._controller.subscribe(self._on_config_change)
        pn.bind(controller.set_value, **self._get_widgets(), watch=True)

    @abstractmethod
    def _on_config_change(self, value: dict[str, Any]) -> None:
        """
        Handle configuration value changes from the service.

        This method should update the widget's display to reflect the new value. The
        controller disables updates to the config service before calling this method,
        so it is safe to update the widget without triggering an infinite loop.

        Parameters
        ----------
        value:
            The new configuration value as a dictionary.
        """

    @abstractmethod
    def _get_widgets(self) -> dict[str, pn.viewable.Viewable]:
        """Return a dictionary of widgets for the configuration fields."""

    @property
    @abstractmethod
    def panel(self) -> pn.viewable.Viewable:
        """
        Get the Panel viewable object for this widget.

        Returns
        -------
        The Panel component that can be displayed in a dashboard.
        """
