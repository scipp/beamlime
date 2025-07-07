# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Any

import pydantic
from pydantic_core import PydanticUndefined

from beamlime.config.workflow_spec import WorkflowId, WorkflowSpec
from beamlime.dashboard.workflow_controller import BoundWorkflowController


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
        for field_name, field_info in model.model_fields.items()
        if field_info.default is not PydanticUndefined
    }


class WorkflowUIHelper:
    """Helper class for workflow UI operations."""

    no_selection = object()

    def __init__(self, controller: BoundWorkflowController):
        """Initialize UI helper with bound controller reference."""
        self._controller = controller

    @staticmethod
    def make_workflow_options(
        specs: dict[WorkflowId, WorkflowSpec] | None = None,
    ) -> dict[str, WorkflowId | object]:
        """
        Get workflow options for selector widget.

        Note that is uses the specs passed as an argument and not the ones from the
        controller.
        """
        specs = specs or {}
        select_text = "--- Click to select a workflow ---"
        options = {select_text: WorkflowUIHelper.no_selection}
        options.update({spec.title: workflow_id for workflow_id, spec in specs.items()})
        return options

    @staticmethod
    def is_no_selection(value: WorkflowId | object) -> bool:
        """Check if the given value represents no workflow selection."""
        return value is WorkflowUIHelper.no_selection

    @staticmethod
    def get_default_workflow_selection() -> object:
        """Get the default value for no workflow selection."""
        return WorkflowUIHelper.no_selection

    def get_workflow_title(self) -> str:
        """Get workflow title from bound controller."""
        return self._controller.spec.title

    def get_workflow_description(self) -> str:
        """Get the description for the bound workflow."""
        return self._controller.spec.description

    def get_source_names(self) -> list[str]:
        """Get all source names the workflow supports."""
        return self._controller.spec.source_names

    def get_initial_source_names(self) -> list[str]:
        """Get initial source names for the bound workflow controller."""
        persistent_config = self._controller.get_persistent_config()
        return persistent_config.source_names if persistent_config else []

    def get_parameter_widget_data(self) -> dict[str, dict[str, Any]]:
        """Get parameter widget data for the bound workflow controller."""
        model_class = self._controller.params_model_class
        previous_values = self.get_initial_parameter_values()
        root_defaults = get_defaults(model_class)
        widget_data = {}

        for field_name, field_info in model_class.model_fields.items():
            field_type: type[pydantic.BaseModel] = field_info.annotation  # type: ignore[assignment]
            values = get_defaults(field_type)
            values.update(root_defaults.get(field_name, {}))
            values.update(previous_values.get(field_name, {}))

            title = field_info.title or field_name.replace('_', ' ').title()
            widget_data[field_name] = {
                'field_type': field_type,
                'values': values,
                'title': title,
                'description': field_info.description,
            }

        return widget_data

    def get_initial_parameter_values(self) -> dict[str, Any]:
        """Get initial parameter values for the bound workflow controller."""
        persistent_config = self._controller.get_persistent_config()
        if not persistent_config:
            return {}
        return persistent_config.config.params

    def assemble_parameter_values(
        self,
        parameter_values: dict[str, pydantic.BaseModel],
    ) -> pydantic.BaseModel:
        """Assemble parameter values into a model for the bound controller."""
        model_class = self._controller.params_model_class
        return model_class(**parameter_values)
