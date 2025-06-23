# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Workflow controller implementation backed by a config service.
"""

import logging
from typing import Any

from beamlime.config.models import ConfigKey
from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpecs,
    WorkflowStatus,
)
from beamlime.dashboard.config_service import ConfigService
from beamlime.dashboard.reduction_widget import WorkflowController

_persistent_configs_key = ConfigKey(
    service_name='dashboard', key='persistent_workflow_configs'
)


class ConfigServiceWorkflowController(WorkflowController):
    """
    Workflow controller backed by a config service.

    This controller manages workflow operations by interacting with a config service
    for starting/stopping workflows and maintaining local state for tracking.
    """

    def __init__(
        self,
        config_service: ConfigService[ConfigKey, dict, Any],
        source_names: list[str] | None = None,
    ) -> None:
        """
        Initialize the workflow controller.

        Parameters
        ----------
        config_service
            Config service for managing workflow configurations
        source_names
            List of source names to monitor for workflow status updates.
            If None, will use a default set of source names.
        """
        self._config_service = config_service
        self._logger = logging.getLogger(__name__)

        # Use provided source names or default set
        self._source_names = source_names or [
            'mantle_detector',
            'endcap_forward_detector',
            'endcap_backward_detector',
            'high_resolution_detector',
        ]

        # Local state tracking - simplified to only track workflows we've started
        self._started_workflows: dict[str, WorkflowId] = {}
        # Temporary status for "Starting..." state before backend responds
        self._starting_workflows: dict[str, WorkflowId] = {}
        # Backend workflow status cache
        self._workflow_status: dict[str, WorkflowStatus] = {}

        self._workflow_specs: WorkflowSpecs = WorkflowSpecs()

        # Callbacks
        self._workflow_specs_callbacks: list[callable] = []
        self._workflow_status_callbacks: list[callable] = []

        # Subscribe to updates
        self._setup_subscriptions()

    def _setup_subscriptions(self) -> None:
        """Setup subscriptions to config service updates."""
        # Subscribe to workflow specs updates
        workflow_specs_key = ConfigKey(
            service_name='data_reduction', key='workflow_specs'
        )

        # Register schema for workflow specs
        self._config_service.register_schema(workflow_specs_key, WorkflowSpecs)

        # Register schema for persistent workflow configs
        persistent_configs_key = ConfigKey(
            service_name='dashboard', key='persistent_workflow_configs'
        )
        self._config_service.register_schema(
            persistent_configs_key, PersistentWorkflowConfigs
        )

        # Subscribe to workflow specs
        self._config_service.subscribe(
            workflow_specs_key, self._on_workflow_specs_updated
        )

        # Subscribe to workflow status for each source
        for source_name in self._source_names:
            workflow_status_key = ConfigKey(
                source_name=source_name,
                service_name='data_reduction',
                key='workflow_status',
            )
            self._config_service.register_schema(workflow_status_key, WorkflowStatus)
            self._config_service.subscribe(
                workflow_status_key,
                lambda status, src=source_name: self._on_workflow_status_updated(
                    src, status
                ),
            )

    def _on_workflow_specs_updated(self, workflow_specs: WorkflowSpecs) -> None:
        """Handle workflow specs updates from config service."""
        self._logger.info(
            'Received workflow specs update with %d workflows',
            len(workflow_specs.workflows),
        )
        self._workflow_specs = workflow_specs

        # Clean up old persistent configs for workflows that no longer exist
        self._cleanup_persistent_configs(set(workflow_specs.workflows.keys()))

        # Notify all subscribers
        for callback in self._workflow_specs_callbacks:
            try:
                callback(workflow_specs)
            except Exception as e:  # noqa: PERF203
                self._logger.error('Error in workflow specs update callback: %s', e)

    def _cleanup_persistent_configs(
        self, current_workflow_ids: set[WorkflowId]
    ) -> None:
        """Clean up persistent configs for workflows that no longer exist."""
        persistent_configs_key = ConfigKey(
            service_name='dashboard', key='persistent_workflow_configs'
        )

        current_configs = self._config_service._config.get(persistent_configs_key)
        if current_configs is None:
            return

        # Clean up and save back if there were changes
        original_count = len(current_configs.configs)
        current_configs.cleanup_missing_workflows(current_workflow_ids)

        if len(current_configs.configs) != original_count:
            self._logger.info(
                'Cleaned up %d obsolete persistent workflow configs',
                original_count - len(current_configs.configs),
            )
            self._config_service.update_config(persistent_configs_key, current_configs)

    def _on_workflow_status_updated(
        self, source_name: str, status: WorkflowStatus
    ) -> None:
        """Handle workflow status updates from config service."""
        self._logger.info(
            'Received workflow status update for source %s: %s',
            source_name,
            status,
        )

        # Update status cache
        self._workflow_status[source_name] = status

        # Remove from starting state if backend has responded
        if source_name in self._starting_workflows:
            del self._starting_workflows[source_name]

        # Update started workflows tracking based on status
        if status in (WorkflowStatus.STARTING, WorkflowStatus.RUNNING):
            # For now, we don't have workflow_id in status, so keep existing mapping
            pass
        elif status in (WorkflowStatus.STOPPED, WorkflowStatus.STARTUP_ERROR):
            # Keep tracking for UI display but workflow is no longer active
            pass

        # Notify all subscribers
        for callback in self._workflow_status_callbacks:
            try:
                callback()
            except Exception as e:  # noqa: PERF203
                self._logger.error('Error in workflow status update callback: %s', e)

    def start_workflow(
        self, workflow_id: WorkflowId, source_names: list[str], config: dict[str, Any]
    ) -> None:
        """Start a workflow with given configuration."""
        self._logger.info(
            'Starting workflow %s on sources %s with config %s',
            workflow_id,
            source_names,
            config,
        )

        workflow_config = WorkflowConfig(identifier=workflow_id, values=config)

        # Update the config for this workflow, used for restoring widget state
        current_configs = self._config_service._config.get(
            _persistent_configs_key, PersistentWorkflowConfigs()
        )
        current_configs.configs[workflow_id] = PersistentWorkflowConfig(
            source_names=source_names, config=workflow_config
        )
        self._config_service.update_config(_persistent_configs_key, current_configs)

        # Send workflow config to each source
        for source_name in source_names:
            config_key = ConfigKey(
                source_name=source_name,
                service_name="data_reduction",
                key="workflow_config",
            )

            # Register schema and update config
            self._config_service.register_schema(config_key, WorkflowConfig)
            self._config_service.update_config(config_key, workflow_config)

            # Update local state - mark as starting
            self._started_workflows[source_name] = workflow_id
            self._starting_workflows[source_name] = workflow_id

    def stop_workflow_for_source(self, source_name: str) -> None:
        """Stop a running workflow for a specific source."""
        self._logger.info('Stopping workflow for source %s', source_name)

        if source_name not in self._started_workflows:
            self._logger.warning('No running workflow found for source %s', source_name)
            return

        # Send None to stop the workflow
        config_key = ConfigKey(
            source_name=source_name,
            service_name="data_reduction",
            key="workflow_config",
        )
        # Register schema and update config
        self._config_service.register_schema(config_key, WorkflowConfig)
        self._config_service.update_config(config_key, WorkflowConfig(identifier=None))

    def remove_workflow_for_source(self, source_name: str) -> None:
        """Remove a stopped workflow from tracking."""
        self._logger.info('Removing workflow for source %s', source_name)

        if source_name in self._started_workflows:
            del self._started_workflows[source_name]

        if source_name in self._workflow_status:
            del self._workflow_status[source_name]

        if source_name in self._starting_workflows:
            del self._starting_workflows[source_name]

    def get_running_workflows(self) -> dict[str, WorkflowId]:
        """Get currently running workflows mapped by source name."""
        return self._started_workflows.copy()

    def get_workflow_status(self, source_name: str) -> WorkflowStatus | None:
        """Get the status of a workflow for a specific source."""
        # Check if we're in starting state (before backend responds)
        if source_name in self._starting_workflows:
            return WorkflowStatus.STARTING

        # Get status from backend
        backend_status = self._workflow_status.get(source_name)
        if backend_status is None:
            # Return UNKNOWN if we have a tracked workflow but no status
            if source_name in self._started_workflows:
                return WorkflowStatus.UNKNOWN
            return None

        return backend_status

    def get_workflow_specs(self) -> WorkflowSpecs:
        """Get the current workflow specifications."""
        return self._workflow_specs

    def subscribe_to_workflow_specs_updates(self, callback: callable) -> None:
        """Subscribe to workflow specs updates."""
        self._workflow_specs_callbacks.append(callback)

    def subscribe_to_workflow_status_updates(self, callback: callable) -> None:
        """Subscribe to workflow status updates."""
        self._workflow_status_callbacks.append(callback)

    def process_config_updates(self) -> None:
        """Process any pending configuration updates from the config service."""
        self._config_service.process_incoming_messages()

    def load_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        """Load saved workflow configuration."""

        # Get all persistent configs
        all_configs = self._config_service._config.get(_persistent_configs_key)
        if all_configs is None:
            return None

        return all_configs.configs.get(workflow_id)
