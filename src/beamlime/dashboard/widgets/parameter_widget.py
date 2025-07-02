# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from typing import Any

import panel as pn

from beamlime.config.workflow_spec import Parameter, ParameterType


class ParameterWidget:
    """Widget for configuring a single workflow parameter."""

    def __init__(self, parameter: Parameter, initial_value: Any | None = None) -> None:
        """
        Initialize parameter widget.

        Parameters
        ----------
        parameter
            Parameter specification
        initial_value
            Initial value for the parameter, if None uses parameter default
        """
        self._parameter = parameter
        self._value = initial_value if initial_value is not None else parameter.default
        self._widget = self._create_widget()

    def _create_widget(self) -> pn.Param:
        """Create the appropriate Panel widget based on parameter type."""
        widget_config = {
            "name": self._parameter.name,
            "value": self._value,
        }

        if self._parameter.unit:
            widget_config["name"] = f"{self._parameter.name} ({self._parameter.unit})"

        if self._parameter.param_type == ParameterType.INT:
            return pn.widgets.IntInput(**widget_config)
        elif self._parameter.param_type == ParameterType.FLOAT:
            return pn.widgets.FloatInput(**widget_config)
        elif self._parameter.param_type == ParameterType.STRING:
            return pn.widgets.TextInput(**widget_config)
        elif self._parameter.param_type == ParameterType.BOOL:
            return pn.widgets.Checkbox(**widget_config)
        elif self._parameter.param_type == ParameterType.OPTIONS:
            widget_config["options"] = self._parameter.options
            return pn.widgets.Select(**widget_config)
        else:
            raise ValueError(
                f"Unsupported parameter type: {self._parameter.param_type}"
            )

    @property
    def widget(self) -> pn.Param:
        """Get the Panel widget."""
        return self._widget

    @property
    def value(self) -> Any:
        """Get the current value of the parameter."""
        return self._widget.value

    @property
    def name(self) -> str:
        """Get the parameter name."""
        return self._parameter.name

    @property
    def description(self) -> str:
        """Get the parameter description."""
        return self._parameter.description
