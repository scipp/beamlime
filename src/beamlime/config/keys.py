# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""Example of a central definition of all configuration keys."""

from beamlime.config.models import StartTime, TOAEdges, TOARange
from beamlime.config.workflow_spec import (
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowSpecs,
    WorkflowStatus,
)

from .schema_registry import ConfigItemSpec

MONITOR_START_TIME = ConfigItemSpec(
    key="start_time",
    service_name="monitor_data",
    model=StartTime,
    description="Start time for the monitor data",
    produced_by={"dashboard"},
    consumed_by={"monitor_data"},
)

MONITOR_TOA_EDGES = ConfigItemSpec(
    key="toa_edges",
    service_name="monitor_data",
    model=TOAEdges,
    description="Time of Arrival edges for a TOA histogram",
    produced_by={"dashboard"},
    consumed_by={"monitor_data"},
)

DETECTOR_TOA_RANGE = ConfigItemSpec(
    key="toa_range",
    service_name="detector_data",
    model=TOARange,
    description="Limit the time-of-arrival range when computing counts/pixel",
    produced_by={"dashboard"},
    consumed_by={"detector_data"},
)

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
