# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""Central definition of all configuration keys."""

from beamlime.config.workflow_spec import (
    WorkflowSpecs,
    WorkflowStatus,
    WorkflowConfig,
    PersistentWorkflowConfigs,
)
from .key_registry import register_key

# Data reduction service keys
WORKFLOW_SPECS = register_key(
    key="workflow_specs",
    service_name="data_reduction",
    model=WorkflowSpecs,
    description="Available workflow specifications",
    produced_by={"data_reduction"},
    consumed_by={"dashboard"},
)

WORKFLOW_STATUS = register_key(
    key="workflow_status",
    service_name="data_reduction",
    model=WorkflowStatus,
    description="Current status of a workflow for a source",
    produced_by={"data_reduction"},
    consumed_by={"dashboard"},
)

WORKFLOW_CONFIG = register_key(
    key="workflow_config",
    service_name="data_reduction",
    model=WorkflowConfig,
    description="Configuration for a workflow",
    produced_by={"dashboard"},
    consumed_by={"data_reduction"},
)

# Dashboard service keys
PERSISTENT_WORKFLOW_CONFIGS = register_key(
    key="persistent_workflow_configs",
    service_name="dashboard",
    model=PersistentWorkflowConfigs,
    description="Saved workflow configurations",
    produced_by={"dashboard"},
    consumed_by={"dashboard"},
)
