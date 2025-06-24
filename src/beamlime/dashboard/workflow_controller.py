# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Workflow controller implementation backed by a config service.
"""

import logging
from collections.abc import Callable
from typing import Any

from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowSpecs,
    WorkflowStatus,
    WorkflowStatusType,
)

from .workflow_config_service import ConfigServiceAdapter, WorkflowConfigService
from .workflow_controller_base import WorkflowControllerBase


class WorkflowController(WorkflowControllerBase):
    """
    Workflow controller backed by a config service.

    This controller manages workflow operations by interacting with a config service
    for starting/stopping workflows and maintaining local state for tracking.

    Brief overview of what this controller does in the wider context of the "data
    reduction" service Kafka:

    - Workflow specs are available/updated from Kafka via the config service.
    - Workflow specs define list of available workflows, their parameters, and
      descriptions.
    - GUI displays available workflows and allows configuring and starting them via
      the controller.
    - Controller persists configs for workflows to allow restoring widget state across
      sessions.
    - Reduction services publish workflow status updates to Kafka.
    - Controller listens for these updates and maintains local state for UI display.
    """

    def __init__(
        self,
        service: WorkflowConfigService,
        source_names: list[str],
    ) -> None:
        """
        Initialize the workflow controller.

        Parameters
        ----------
        service
            Service for managing workflow configurations
        source_names
            List of source names to monitor for workflow status updates.
        """
        self._service = service
        self._logger = logging.getLogger(__name__)

        self._source_names = source_names

        # Initialize all sources with UNKNOWN status
        self._workflow_status: dict[str, WorkflowStatus] = {
            source_name: WorkflowStatus(source_name=source_name)
            for source_name in self._source_names
        }

        self._workflow_specs: WorkflowSpecs = WorkflowSpecs()

        # Callbacks
        self._workflow_specs_callbacks: list[
            Callable[[dict[WorkflowId, WorkflowSpec]], None]
        ] = []
        self._workflow_status_callbacks: list[
            Callable[[dict[str, WorkflowStatus]], None]
        ] = []

        # Subscribe to updates
        self._setup_subscriptions()

    @classmethod
    def from_config_service(
        cls,
        *,
        config_service,
        source_names: list[str],
    ) -> 'WorkflowController':
        """Create WorkflowController from ConfigService."""
        adapter = ConfigServiceAdapter(config_service, source_names)
        return cls(adapter, source_names)

    def _setup_subscriptions(self) -> None:
        """Setup subscriptions to service updates."""
        # Subscribe to workflow specs
        self._service.subscribe_to_workflow_specs(self._update_workflow_specs)

        # Subscribe to workflow status for each source
        for source_name in self._source_names:
            self._service.subscribe_to_workflow_status(
                source_name, self._update_workflow_status
            )

    def _update_workflow_specs(self, workflow_specs: WorkflowSpecs) -> None:
        """Handle workflow specs updates from service."""
        self._logger.info(
            'Received workflow specs update with %d workflows',
            len(workflow_specs.workflows),
        )
        self._workflow_specs = workflow_specs

        # Clean up old persistent configs for workflows that no longer exist
        self._cleanup_persistent_configs(set(workflow_specs.workflows.keys()))

        # Notify all subscribers with the specs
        for callback in self._workflow_specs_callbacks:
            self._notify_workflow_specs_update(callback)

    def _update_workflow_status(self, status: WorkflowStatus) -> None:
        """Handle workflow status updates from service."""
        self._logger.info('Received workflow status update: %s', status)
        self._workflow_status[status.source_name] = status
        for callback in self._workflow_status_callbacks:
            self._notify_workflow_status_update(callback)

    def _cleanup_persistent_configs(
        self, current_workflow_ids: set[WorkflowId]
    ) -> None:
        """Clean up persistent configs for workflows that no longer exist."""
        current_configs = self._service.get_persistent_configs()

        # Clean up and save back if there were changes
        original_count = len(current_configs.configs)
        current_configs.cleanup_missing_workflows(current_workflow_ids)

        if len(current_configs.configs) != original_count:
            self._logger.info(
                'Cleaned up %d obsolete persistent workflow configs',
                original_count - len(current_configs.configs),
            )
            self._service.save_persistent_configs(current_configs)

    def start_workflow(
        self, workflow_id: WorkflowId, source_names: list[str], config: dict[str, Any]
    ) -> bool:
        """Start a workflow with given configuration.

        Returns True if the workflow was started successfully, False otherwise.
        """
        # Check if workflow exists
        if workflow_id not in self._workflow_specs.workflows:
            self._logger.warning(
                'Cannot start workflow %s: workflow does not exist', workflow_id
            )
            return False

        self._logger.info(
            'Starting workflow %s on sources %s with config %s',
            workflow_id,
            source_names,
            config,
        )

        workflow_config = WorkflowConfig(identifier=workflow_id, values=config)

        # Update the config for this workflow, used for restoring widget state
        current_configs = self._service.get_persistent_configs()
        current_configs.configs[workflow_id] = PersistentWorkflowConfig(
            source_names=source_names, config=workflow_config
        )
        self._service.save_persistent_configs(current_configs)

        # Send workflow config to each source
        for source_name in source_names:
            self._service.send_workflow_config(source_name, workflow_config)

            # Set status to STARTING for immediate UI feedback
            self._workflow_status[source_name] = WorkflowStatus(
                source_name=source_name,
                workflow_id=workflow_id,
                status=WorkflowStatusType.STARTING,
            )
        # Notify once, will update whole list of source names
        for callback in self._workflow_status_callbacks:
            self._notify_workflow_status_update(callback)

        return True

    def stop_workflow_for_source(self, source_name: str) -> None:
        """Stop a running workflow for a specific source."""
        self._logger.info('Stopping workflow for source %s', source_name)

        # Send None to stop the workflow
        self._service.send_workflow_config(source_name, WorkflowConfig(identifier=None))
        # Update status to STOPPING for immediate UI feedback
        self._update_workflow_status(
            WorkflowStatus(source_name=source_name, status=WorkflowStatusType.STOPPING)
        )

    def remove_workflow_for_source(self, source_name: str) -> None:
        """Remove a stopped workflow from tracking."""
        self._logger.info('Removing workflow for source %s', source_name)
        # Reset status to UNKNOWN (back to initial state)
        self._update_workflow_status(WorkflowStatus(source_name=source_name))

    def get_workflow_spec(self, workflow_id: WorkflowId) -> WorkflowSpec | None:
        """Get the current workflow specification for the given Id."""
        return self._workflow_specs.workflows.get(workflow_id)

    def get_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        """Load saved workflow configuration."""
        all_configs = self._service.get_persistent_configs()
        return all_configs.configs.get(workflow_id)

    def subscribe_to_workflow_updates(
        self, callback: Callable[[dict[WorkflowId, WorkflowSpec]], None]
    ) -> None:
        """Subscribe to workflow updates."""
        self._workflow_specs_callbacks.append(callback)
        self._notify_workflow_specs_update(callback)

    def subscribe_to_workflow_status_updates(
        self, callback: Callable[[dict[str, WorkflowStatus]], None]
    ) -> None:
        """Subscribe to workflow status updates."""
        self._workflow_status_callbacks.append(callback)
        self._notify_workflow_status_update(callback)

    def _notify_workflow_specs_update(
        self, callback: Callable[[dict[WorkflowId, WorkflowSpec]], None]
    ) -> None:
        """Notify a single subscriber about workflow specs update."""
        try:
            callback(self._workflow_specs.workflows.copy())
        except Exception as e:
            self._logger.error('Error in workflow specs update callback: %s', e)

    def _notify_workflow_status_update(
        self, callback: Callable[[dict[str, WorkflowStatus]], None]
    ):
        try:
            callback(self._workflow_status.copy())
        except Exception as e:
            self._logger.error('Error in workflow status update callback: %s', e)
