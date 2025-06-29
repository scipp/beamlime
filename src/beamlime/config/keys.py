# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""Example of a central definition of all configuration keys."""

from beamlime.config.workflow_spec import (
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowSpecs,
    WorkflowStatus,
)

from .key_registry import ConfigItemSpec

# Data reduction service keys
WORKFLOW_SPECS = ConfigItemSpec(
    key="workflow_specs",
    service_name="data_reduction",
    model=WorkflowSpecs,
    description="Available workflow specifications",
    produced_by={"data_reduction"},
    consumed_by={"dashboard"},
)

WORKFLOW_STATUS = ConfigItemSpec(
    key="workflow_status",
    service_name="data_reduction",
    model=WorkflowStatus,
    description="Current status of a workflow for a source",
    produced_by={"data_reduction"},
    consumed_by={"dashboard"},
)

WORKFLOW_CONFIG = ConfigItemSpec(
    key="workflow_config",
    service_name="data_reduction",
    model=WorkflowConfig,
    description="Configuration for a workflow",
    produced_by={"dashboard"},
    consumed_by={"data_reduction"},
)

# Dashboard service keys
PERSISTENT_WORKFLOW_CONFIGS = ConfigItemSpec(
    key="persistent_workflow_configs",
    service_name="dashboard",
    model=PersistentWorkflowConfigs,
    description="Saved workflow configurations",
    produced_by={"dashboard"},
    consumed_by={"dashboard"},
)

# Usage patterns:
#
# Getting model type: WORKFLOW_CONFIG.model
# Creating ConfigKey: WORKFLOW_CONFIG.create_key(source_name="my_source")
