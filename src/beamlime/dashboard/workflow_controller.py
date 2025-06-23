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
)
from beamlime.dashboard.config_service import ConfigService
from beamlime.dashboard.reduction_widget import WorkflowController, WorkflowStatus

_persistent_configs_key = ConfigKey(
    service_name='dashboard', key='persistent_workflow_configs'
)


class ConfigServiceWorkflowController(WorkflowController):
    """
    Workflow controller backed by a config service.

    This controller manages workflow operations by interacting with a config service
    for starting/stopping workflows and maintaining local state for tracking.
    """

    def __init__(self, config_service: ConfigService[ConfigKey, dict, Any]) -> None:
        """
        Initialize the workflow controller.

        Parameters
        ----------
        config_service
            Config service for managing workflow configurations
        """
        self._config_service = config_service
        self._logger = logging.getLogger(__name__)

        # Local state tracking
        self._running_workflows: dict[str, WorkflowId] = {}
        self._workflow_status: dict[str, WorkflowStatus] = {}
        self._workflow_specs: WorkflowSpecs = WorkflowSpecs()

        # Callback for workflow specs updates
        self._workflow_specs_callbacks: list[callable] = []

        # Subscribe to workflow specs updates
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

        # Subscribe to updates
        self._config_service.subscribe(
            workflow_specs_key, self._on_workflow_specs_updated
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

            # Update local state
            self._running_workflows[source_name] = workflow_id
            self._workflow_status[source_name] = WorkflowStatus.RUNNING

    def stop_workflow_for_source(self, source_name: str) -> None:
        """Stop a running workflow for a specific source."""
        self._logger.info('Stopping workflow for source %s', source_name)

        if source_name not in self._running_workflows:
            self._logger.warning('No running workflow found for source %s', source_name)
            return

        # Send None to stop the workflow
        config_key = ConfigKey(
            source_name=source_name,
            service_name="data_reduction",
            key="workflow_config",
        )

        # We need to send None, but config service expects pydantic models
        # For stopping workflows, we'll create a special "empty" config
        # or handle this case in the backend
        # For now, we'll update local state and let the backend handle None values
        # through a different mechanism if needed
        _ = config_key  # To avoid unused variable warning

        # Update local state to stopped
        self._workflow_status[source_name] = WorkflowStatus.STOPPED

        # Note: Actual stopping mechanism would depend on how the backend
        # handles workflow termination. This might require a separate
        # stop command or setting a special flag in the config.

    def remove_workflow_for_source(self, source_name: str) -> None:
        """Remove a stopped workflow from tracking."""
        self._logger.info('Removing workflow for source %s', source_name)

        if source_name in self._running_workflows:
            del self._running_workflows[source_name]

        if source_name in self._workflow_status:
            del self._workflow_status[source_name]

    def get_running_workflows(self) -> dict[str, WorkflowId]:
        """Get currently running workflows mapped by source name."""
        return self._running_workflows.copy()

    def get_workflow_status(self, source_name: str) -> WorkflowStatus | None:
        """Get the status of a workflow for a specific source."""
        return self._workflow_status.get(source_name)

    def get_workflow_specs(self) -> WorkflowSpecs:
        """Get the current workflow specifications."""
        return self._workflow_specs

    def subscribe_to_workflow_specs_updates(self, callback: callable) -> None:
        """Subscribe to workflow specs updates."""
        self._workflow_specs_callbacks.append(callback)

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
