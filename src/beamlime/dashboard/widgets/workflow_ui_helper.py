# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Any

import pydantic
from pydantic_core import PydanticUndefined

from beamlime.config.workflow_spec import WorkflowId, WorkflowSpec
from beamlime.dashboard.workflow_controller import (
    BoundWorkflowController,
    WorkflowController,
)


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

    def __init__(self, controller: WorkflowController):
        """Initialize UI helper with controller reference."""
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

    def is_no_selection(self, value: WorkflowId | object) -> bool:
        """Check if the given value represents no workflow selection."""
        return value is self.no_selection

    def get_default_workflow_selection(self) -> object:
        """Get the default value for no workflow selection."""
        return self.no_selection

    def get_workflow_title(self, workflow_id: WorkflowId | None) -> str:
        """Get workflow title from ID, fallback to ID if not found."""
        if workflow_id is None:
            return "None"
        if (spec := self._controller.get_workflow_spec(workflow_id)) is not None:
            return spec.title
        return str(workflow_id)

    def get_workflow_description(self, workflow_id: WorkflowId | object) -> str | None:
        """
        Get the description for a workflow ID or selection value.

        Returns `None` if no workflow is selected.
        """
        if not isinstance(workflow_id, WorkflowId) or self.is_no_selection(workflow_id):
            return None
        if (spec := self._controller.get_workflow_spec(workflow_id)) is None:
            return None
        return spec.description

    @staticmethod
    def get_initial_source_names(
        bound_controller: BoundWorkflowController,
    ) -> list[str]:
        """Get initial source names for a bound workflow controller."""
        persistent_config = bound_controller.get_persistent_config()
        return persistent_config.source_names if persistent_config else []

    @staticmethod
    def get_parameter_widget_data(
        bound_controller: BoundWorkflowController,
    ) -> dict[str, dict[str, Any]]:
        """Get parameter widget data for a bound workflow controller."""
        model_class = bound_controller.params_model_class
        previous_values = WorkflowUIHelper.get_initial_parameter_values(
            bound_controller
        )
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

    @staticmethod
    def get_initial_parameter_values(
        bound_controller: BoundWorkflowController,
    ) -> dict[str, Any]:
        """Get initial parameter values for a bound workflow controller."""
        persistent_config = bound_controller.get_persistent_config()
        if not persistent_config:
            return {}
        return persistent_config.config.params

    @staticmethod
    def assemble_parameter_values(
        bound_controller: BoundWorkflowController,
        parameter_values: dict[str, pydantic.BaseModel],
    ) -> pydantic.BaseModel:
        """Assemble parameter values into a model for a bound controller."""
        model_class = bound_controller.params_model_class
        return model_class(**parameter_values)
