# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from typing import Any

import pydantic
from pydantic_core import PydanticUndefined

from beamlime.config.workflow_spec import WorkflowId, WorkflowSpec
from beamlime.dashboard.workflow_controller import WorkflowController


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
        options.update({spec.name: workflow_id for workflow_id, spec in specs.items()})
        return options

    def is_no_selection(self, value: WorkflowId | object) -> bool:
        """Check if the given value represents no workflow selection."""
        return value is self.no_selection

    def get_default_workflow_selection(self) -> object:
        """Get the default value for no workflow selection."""
        return self.no_selection

    def get_workflow_name(self, workflow_id: WorkflowId | None) -> str:
        """Get workflow name from ID, fallback to ID if not found."""
        if workflow_id is None:
            return "None"
        if (spec := self._controller.get_workflow_spec(workflow_id)) is not None:
            return spec.name
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

    def get_initial_parameter_values(self, workflow_id: WorkflowId) -> dict[str, Any]:
        """Get initial parameter values for a workflow."""
        persistent_config = self._controller.get_workflow_config(workflow_id)
        if not persistent_config:
            return {}
        return persistent_config.config.params

    def get_initial_source_names(self, workflow_id: WorkflowId) -> list[str]:
        """Get initial source names for a workflow."""
        persistent_config = self._controller.get_workflow_config(workflow_id)
        return persistent_config.source_names if persistent_config else []

    def get_workflow_model_class(
        self, workflow_id: WorkflowId
    ) -> type[pydantic.BaseModel]:
        """Get and validate workflow parameter model class."""
        model_class = self._controller.get_workflow_params(workflow_id)
        if model_class is None:
            raise ValueError(
                f"Workflow parameters for '{workflow_id}' are not defined."
            )
        return model_class

    def get_parameter_widget_data(
        self, workflow_id: WorkflowId
    ) -> dict[str, dict[str, Any]]:
        """Get parameter widget data for a workflow."""
        model_class = self.get_workflow_model_class(workflow_id)
        previous_values = self.get_initial_parameter_values(workflow_id)
        root_defaults = get_defaults(model_class)
        widget_data = {}

        for field_name, field_info in model_class.model_fields.items():
            field_type: type[pydantic.BaseModel] = field_info.annotation  # type: ignore[assignment]
            values = get_defaults(field_type)
            values.update(root_defaults.get(field_name, {}))
            values.update(previous_values.get(field_name, {}))

            # Extract title and description from field info
            title = (
                getattr(field_info, 'title', None)
                or field_name.replace('_', ' ').title()
            )
            description = getattr(field_info, 'description', None)

            widget_data[field_name] = {
                'field_type': field_type,
                'values': values,
                'title': title,
                'description': description,
            }

        return widget_data

    def assemble_parameter_values(
        self, workflow_id: WorkflowId, parameter_values: dict[str, pydantic.BaseModel]
    ) -> pydantic.BaseModel:
        """Assemble parameter values into a model."""
        model_class = self.get_workflow_model_class(workflow_id)
        return model_class(**parameter_values)
