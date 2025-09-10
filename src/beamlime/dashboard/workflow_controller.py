# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Workflow controller implementation backed by a config service.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Mapping

import pydantic

from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    ResultKey,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowStatus,
    WorkflowStatusType,
)

from .data_service import DataService
from .workflow_config_service import ConfigServiceAdapter, WorkflowConfigService


class BoundWorkflowController:
    """
    Controller bound to a specific workflow, providing direct access without ID checks.

    This controller is created from a main WorkflowController and provides a simplified
    interface for widgets that work with a single workflow.
    """

    def __init__(
        self,
        workflow_id: WorkflowId,
        spec: WorkflowSpec,
        main_controller: WorkflowController,
    ) -> None:
        """
        Initialize bound controller.

        Parameters
        ----------
        workflow_id
            The workflow ID this controller is bound to
        spec
            The workflow specification
        main_controller
            Reference to the main controller for operations
        """
        self._workflow_id = workflow_id
        self._spec = spec
        self._main_controller = main_controller

    @property
    def workflow_id(self) -> WorkflowId:
        """Get the workflow ID."""
        return self._workflow_id

    @property
    def spec(self) -> WorkflowSpec:
        """Get the workflow specification."""
        return self._spec

    @property
    def params_model_class(self) -> type[pydantic.BaseModel]:
        """Get the parameters model class."""
        return self._spec.params

    def get_persistent_config(self) -> PersistentWorkflowConfig | None:
        """Get persistent configuration for this workflow."""
        return self._main_controller.get_workflow_config(self._workflow_id)

    def start_workflow(
        self, source_names: list[str], config: pydantic.BaseModel
    ) -> bool:
        """Start this workflow with given configuration."""
        return self._main_controller.start_workflow(
            self._workflow_id, source_names, config
        )


class WorkflowController:
    """
    Workflow controller backed by a config service.

    This controller manages workflow operations by interacting with a config service
    for starting/stopping workflows and maintaining local state for tracking.

    Brief overview of what this controller does in the wider context of the "data
    reduction" service Kafka:

    - Workflow specs are defined in the workflow registry passed to the controller.
    - GUI displays available workflows and allows configuring and starting them via
      the controller.
    - Controller persists configs for workflows to allow restoring widget state across
      sessions.
    - Reduction services publish workflow status updates to Kafka.
    - Controller listens for these updates and maintains local state for UI display.
    """

    def __init__(
        self,
        *,
        service: WorkflowConfigService,
        source_names: list[str],
        workflow_registry: Mapping[WorkflowId, WorkflowSpec],
        data_service: DataService[ResultKey, object] | None = None,
    ) -> None:
        """
        Initialize the workflow controller.

        Parameters
        ----------
        service
            Service for managing workflow configurations
        source_names
            List of source names to monitor for workflow status updates.
        workflow_registry
            Registry of available workflows and their specifications.
        data_service
            Optional data service for cleaning up workflow data keys.
        """
        self._service = service
        self._logger = logging.getLogger(__name__)

        self._source_names = source_names
        self._workflow_registry = workflow_registry
        self._data_service = data_service

        # Initialize all sources with UNKNOWN status
        self._workflow_status: dict[str, WorkflowStatus] = {
            source_name: WorkflowStatus(source_name=source_name)
            for source_name in self._source_names
        }

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
        workflow_registry: Mapping[WorkflowId, WorkflowSpec],
        data_service: DataService[ResultKey, object] | None = None,
    ) -> WorkflowController:
        """Create WorkflowController from ConfigService."""
        return cls(
            service=ConfigServiceAdapter(config_service),
            source_names=source_names,
            workflow_registry=workflow_registry,
            data_service=data_service,
        )

    def _setup_subscriptions(self) -> None:
        """Setup subscriptions to service updates."""
        # Subscribe to workflow status for each source
        for source_name in self._source_names:
            self._service.subscribe_to_workflow_status(
                source_name, self._update_workflow_status
            )

    def _update_workflow_status(self, status: WorkflowStatus) -> None:
        """Handle workflow status updates from service."""
        self._logger.info('Received workflow status update: %s', status)
        self._workflow_status[status.source_name] = status
        for callback in self._workflow_status_callbacks:
            self._notify_workflow_status_update(callback)

    def start_workflow(
        self,
        workflow_id: WorkflowId,
        source_names: list[str],
        config: pydantic.BaseModel,
    ) -> bool:
        """Start a workflow with given configuration.

        Returns True if the workflow was started successfully, False otherwise.
        """
        # Check if workflow exists
        if workflow_id not in self._workflow_registry:
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

        spec = self.get_workflow_spec(workflow_id)
        if spec is None:
            self._logger.error(
                'Workflow spec for %s not found, cannot start workflow', workflow_id
            )
            return False

        # We generate a new job number for the workflow. This will allow for associating
        # multiple jobs with the same workflow run for different sources.
        workflow_config = WorkflowConfig(
            identifier=workflow_id, job_number=uuid.uuid4(), params=config.model_dump()
        )

        # Update the config for this workflow, used for restoring widget state
        current_configs = self._service.get_persistent_configs()
        # Clean up in case there are stale workflows that no longer exist
        current_configs.cleanup_missing_workflows(set(self._workflow_registry))
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

        # Remove associated data keys from data service
        if self._data_service is not None:
            keys_to_remove = list(self._data_service.keys())
            for key in keys_to_remove:
                self._logger.debug('Removing data key: %s', key)
                del self._data_service[key]

        # Reset status to UNKNOWN (back to initial state)
        self._update_workflow_status(WorkflowStatus(source_name=source_name))

    def get_workflow_specs(self) -> dict[WorkflowId, WorkflowSpec]:
        """Get the current workflow specifications sorted by title."""
        return dict(
            sorted(self._workflow_registry.items(), key=lambda item: item[1].title)
        )

    def get_workflow_spec(self, workflow_id: WorkflowId) -> WorkflowSpec | None:
        """Get the current workflow specification for the given Id."""
        return self._workflow_registry.get(workflow_id)

    def get_workflow_params(
        self, workflow_id: WorkflowId
    ) -> type[pydantic.BaseModel] | None:
        """Get the parameters for the given workflow Id."""
        if (workflow_spec := self.get_workflow_spec(workflow_id)) is None:
            return None
        return workflow_spec.params

    def get_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        """Load saved workflow configuration."""
        all_configs = self._service.get_persistent_configs()
        return all_configs.configs.get(workflow_id)

    def subscribe_to_workflow_status_updates(
        self, callback: Callable[[dict[str, WorkflowStatus]], None]
    ) -> None:
        """Subscribe to workflow status updates."""
        self._workflow_status_callbacks.append(callback)
        self._notify_workflow_status_update(callback)

    def _notify_workflow_status_update(
        self, callback: Callable[[dict[str, WorkflowStatus]], None]
    ):
        try:
            callback(self._workflow_status.copy())
        except Exception as e:
            self._logger.error('Error in workflow status update callback: %s', e)

    def get_bound_controller(
        self, workflow_id: WorkflowId
    ) -> BoundWorkflowController | None:
        """
        Get a controller bound to a specific workflow.

        Parameters
        ----------
        workflow_id
            ID of the workflow to bind to

        Returns
        -------
        BoundWorkflowController | None
            Bound controller if workflow exists, None otherwise
        """
        if (spec := self.get_workflow_spec(workflow_id)) is None:
            return None
        return BoundWorkflowController(workflow_id, spec, self)
