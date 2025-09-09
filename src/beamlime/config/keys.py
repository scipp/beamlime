# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""
Central definition of all configuration keys.

Usage patterns:

- Getting model type: WORKFLOW_CONFIG.model
- Creating ConfigKey: WORKFLOW_CONFIG.create_key(source_name="my_source")
"""

from beamlime.config.models import StartTime
from beamlime.config.workflow_spec import (
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowStatus,
)

from .schema_registry import get_schema_registry

_registry = get_schema_registry()

MONITOR_START_TIME = _registry.create(
    key="start_time",
    service_name="monitor_data",
    model=StartTime,
    description="Start time for the monitor data",
    produced_by={"dashboard"},
    consumed_by={"monitor_data"},
)

DETECTOR_START_TIME = _registry.create(
    key="start_time",
    service_name="detector_data",
    model=StartTime,
    description="Start time for the detector data",
    produced_by={"dashboard"},
    consumed_by={"detector_data"},
)

REDUCTION_START_TIME = _registry.create(
    key="start_time",
    service_name="data_reduction",
    model=StartTime,
    description="Start time for the data reduction",
    produced_by={"data_reduction"},
    consumed_by={"dashboard"},
)

# Data reduction service keys
WORKFLOW_STATUS = _registry.create(
    key="workflow_status",
    service_name="job_server",
    model=WorkflowStatus,
    description="Current status of a workflow for a source",
    produced_by={"data_reduction"},
    consumed_by={"dashboard"},
)

# Backend now filters based on instrument name (part of the identifier). This
# mechanism will likely change again in the future, but for now service_name=None so
# all backend services receive this.
WORKFLOW_CONFIG = _registry.create(
    key="workflow_config",
    service_name=None,
    model=WorkflowConfig,
    description="Configuration for a workflow",
    produced_by={"dashboard"},
    consumed_by={"data_reduction"},
)

# Dashboard service keys
PERSISTENT_WORKFLOW_CONFIGS = _registry.create(
    key="persistent_workflow_configs",
    service_name="dashboard",
    model=PersistentWorkflowConfigs,
    description="Saved workflow configurations",
    produced_by={"dashboard"},
    consumed_by={"dashboard"},
)
