# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import panel as pn

from beamlime.dashboard.controller_factory import Controller


class ConfigWidget:
    """
    Widget for configuring a single config value.

    As config values are defined as pydantic models, each widget can be composed of
    sub-widgets.
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
