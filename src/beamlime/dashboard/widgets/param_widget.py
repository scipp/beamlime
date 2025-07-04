# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from enum import Enum
from pathlib import Path
from typing import Any

import panel as pn
import pydantic
from pydantic_core import PydanticUndefined


def snake_to_camel(snake_str: str) -> str:
    """Convert snake_case to camelCase."""
    components = snake_str.split('_')
    return ''.join(word.capitalize() for word in components)


def get_defaults(model: type[pydantic.BaseModel]) -> dict[str, Any]:
    """
    Get default values for all fields in a Pydantic model.

    Parameters
    ----------
    model
        Pydantic model class

    Returns
    -------
    dict[str, Any]
        Dictionary of field names and their default values
    """
    return {
        field_name: field_info.default
        if field_info.default is not PydanticUndefined
        else None
        for field_name, field_info in model.model_fields.items()
    }


class ParamWidget:
    """Widget for creating and validating Pydantic model instances."""

    def __init__(self, model_class: type[pydantic.BaseModel]):
        self.model_class = model_class
        self.widgets = {}
        self._create_widgets()
        self.layout = pn.Row(*self.widgets.values())

    def _create_widgets(self):
        """Create Panel widgets for each field in the model."""
        for field_name, field_info in self.model_class.model_fields.items():
            widget = self._create_widget_for_field(field_name, field_info)
            self.widgets[field_name] = widget

    def _create_widget_for_field(
        self, field_name: str, field_info: pydantic.fields.FieldInfo
    ):
        """Create appropriate widget based on field type."""
        field_type = field_info.annotation
        default_value = (
            field_info.default if field_info.default is not PydanticUndefined else None
        )
        description = field_info.description or field_name
        display_name = snake_to_camel(field_name)

        # Handle Optional types and get the actual type
        origin = getattr(field_type, '__origin__', None)
        if origin is type(None) or (
            hasattr(field_type, '__args__') and type(None) in field_type.__args__
        ):
            # Extract non-None type from Union
            args = getattr(field_type, '__args__', ())
            field_type = next(
                (arg for arg in args if arg is not type(None)), field_type
            )

        # Create widget based on type
        if field_type is float:
            return pn.widgets.FloatInput(
                name=display_name,
                value=default_value or 0.0,
                placeholder=description,
                width=100,
                description=description,
            )
        elif field_type is int:
            return pn.widgets.IntInput(
                name=display_name,
                value=default_value or 0,
                placeholder=description,
                width=100,
                description=description,
            )
        elif field_type is bool:
            # Does not support description directly
            return pn.widgets.Checkbox(
                name=display_name,
                value=default_value or False,
                margin=(20, 5, 5, 5),
            )
        elif field_type == Path or field_type is str:
            return pn.widgets.TextInput(
                name=display_name,
                value=str(default_value) if default_value else "",
                placeholder=description,
                description=description,
            )
        elif isinstance(field_type, type) and issubclass(field_type, Enum):
            options = {enum_val.value: enum_val for enum_val in field_type}
            default_display = (
                default_value.value if default_value else next(iter(options.keys()))
            )
            return pn.widgets.Select(
                name=display_name,
                options=options,
                value=default_display,
                width=100,
                description=description,
            )
        else:
            # Fallback to text input
            return pn.widgets.TextInput(
                name=display_name,
                value=str(default_value) if default_value else "",
                placeholder=description,
                description=description,
            )

    def get_values(self) -> dict[str, Any]:
        """Get current values from all widgets."""
        values = {}
        for field_name, widget in self.widgets.items():
            value = widget.value
            # Convert Path strings back to Path objects
            field_type = self.model_class.model_fields[field_name].annotation
            if field_type == Path and isinstance(value, str):
                value = Path(value) if value else None
            # Handle enum values - widget.value will be the enum instance for Select
            # widgets with enum options
            elif isinstance(field_type, type) and issubclass(field_type, Enum):
                # value is already the enum instance from the Select widget
                pass
            values[field_name] = value
        return values

    def set_values(self, values: dict[str, Any]):
        """Set values for widgets."""
        for field_name, value in values.items():
            if field_name in self.widgets:
                field_type = self.model_class.model_fields[field_name].annotation
                if isinstance(value, Path):
                    value = str(value)
                elif (
                    isinstance(field_type, type)
                    and issubclass(field_type, Enum)
                    and isinstance(value, field_type)
                ):
                    # For enum fields, set the string value to display
                    value = value.value
                self.widgets[field_name].value = value

    def create_model(self) -> pydantic.BaseModel:
        """Create and validate a model instance from current widget values."""
        values = self.get_values()
        return self.model_class(**values)

    def validate(self) -> tuple[bool, str]:
        """Validate current values without creating the model."""
        try:
            self.create_model()
            return True, "Valid"
        except pydantic.ValidationError as e:
            return False, str(e)

    def panel(self):
        """Return the Panel layout."""
        return self.layout
