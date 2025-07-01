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
        self._setup_widget_handlers()

    @abstractmethod
    def _on_config_change(self, value: dict[str, Any]) -> None:
        """
        Handle configuration value changes from the service.

        This method should update the widget's display to reflect the new value.

        Parameters
        ----------
        value:
            The new configuration value as a dictionary.
        """

    @abstractmethod
    def _setup_widget_handlers(self) -> None:
        """
        Set up handlers for widget value changes.

        This method should connect widget change events to call
        self._on_widget_change() when the user modifies the widget.
        """

    def _on_widget_change(self, value: dict[str, Any]) -> None:
        """
        Handle widget value changes from user interaction.

        This method updates the configuration service with the new value.

        Parameters
        ----------
        value:
            The new value from the widget as a dictionary.
        """
        self._controller.set_value(value)

    @property
    @abstractmethod
    def panel(self) -> pn.viewable.Viewable:
        """
        Get the Panel viewable object for this widget.

        Returns
        -------
        The Panel component that can be displayed in a dashboard.
        """
        ...
