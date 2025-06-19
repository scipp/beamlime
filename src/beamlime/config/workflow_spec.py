# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""
Models for data reduction workflow widget creation and configuration.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field, model_validator

T = TypeVar('T')


class ParameterType(str, Enum):
    """
    Enum for parameter types.

    This enum is used to define the type of a parameter.
    """

    INT = 'int'
    FLOAT = 'float'
    STRING = 'string'
    BOOL = 'bool'
    OPTIONS = 'options'


class Parameter(BaseModel, Generic[T]):
    """
    Model for workflow parameter.

    This model is used to define a parameter for a specific workflow.
    """

    name: str = Field(description="Name of the parameter.")
    unit: str | None = Field(default=None, description="Unit of the parameter.")
    description: str = Field(description="Description of the parameter.")
    param_type: ParameterType = Field(description="Type of the parameter.")
    default: T = Field(description="Default value of the parameter.")
    options: list[T] | None = Field(
        default=None,
        description="List of options, if it is a parameter with options.",
    )

    @model_validator(mode='after')
    def validate_options(self) -> Parameter:
        """Validate that options are provided for OPTIONS parameter type."""
        if self.param_type == ParameterType.OPTIONS:
            if self.options is None or not self.options:
                raise ValueError(
                    "Options must be provided for parameter type 'OPTIONS'"
                )
            if self.default not in set(self.options):
                raise ValueError(
                    f"Default {self.default} must be one of options {self.options}"
                )
        elif self.options is not None:
            raise ValueError(
                "Options must be None for parameter types other than 'OPTIONS'"
            )
        return self


class WorkflowSpec(BaseModel):
    """
    Model for workflow specification.

    This model is used to define a workflow and its parameters. Beamlime publishes
    workflow specifications to Kafka, which can be used to create user interfaces for
    configuring workflows.
    """

    name: str = Field(description="Name of the workflow.")
    description: str = Field(description="Description of the workflow.")
    source_names: list[str] = Field(
        default_factory=list,
        description="List of detectors the workflow can be applied to.",
    )
    parameters: list[Parameter] = Field(
        default_factory=list, description="Parameters for the workflow."
    )


WorkflowId = str


class WorkflowSpecs(BaseModel):
    """
    Model for workflow specifications.

    This model is used to define multiple workflows and their parameters.
    """

    workflows: dict[WorkflowId, WorkflowSpec] = Field(
        default_factory=dict, description="Workflows and their parameters."
    )


class WorkflowConfig(BaseModel):
    """
    Model for workflow configuration.

    This model is used to set the parameter values for a specific workflow. The values
    correspond to the parameters defined in the workflow specification
    :py:class:`WorkflowSpec`.
    """

    identifier: WorkflowId = Field(
        description="Hash of the workflow, used to identify the workflow."
    )
    values: dict[str, Any] = Field(
        default_factory=dict, description="Parameter values for the workflow."
    )
