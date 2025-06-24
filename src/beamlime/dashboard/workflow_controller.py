# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Workflow controller implementation backed by a config service.
"""

import logging
from typing import Any, Protocol

from beamlime.config.models import ConfigKey
from beamlime.config.workflow_spec import (
    PersistentWorkflowConfig,
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowId,
    WorkflowSpec,
    WorkflowSpecs,
    WorkflowStatus,
    WorkflowStatusType,
)

from .workflow_controller_base import WorkflowControllerBase

_persistent_configs_key = ConfigKey(
    service_name='dashboard', key='persistent_workflow_configs'
)


class WorkflowConfigService(Protocol):
    """Protocol for workflow controller dependencies."""

    def get_workflow_specs(self) -> WorkflowSpecs:
        """Get current workflow specifications."""
        ...

    def get_persistent_configs(self) -> PersistentWorkflowConfigs:
        """Get persistent workflow configurations."""
        ...

    def save_persistent_configs(self, configs: PersistentWorkflowConfigs) -> None:
        """Save persistent workflow configurations."""
        ...

    def send_workflow_config(self, source_name: str, config: WorkflowConfig) -> None:
        """Send workflow configuration to a source."""
        ...

    def subscribe_to_workflow_specs(self, callback: callable) -> None:
        """Subscribe to workflow specs updates."""
        ...

    def subscribe_to_workflow_status(
        self, source_name: str, callback: callable
    ) -> None:
        """Subscribe to workflow status updates for a source."""
        ...


class ConfigServiceAdapter:
    """Adapter to make ConfigService compatible with WorkflowConfigService protocol."""

    def __init__(self, config_service, source_names: list[str]):
        self._config_service = config_service
        self._source_names = source_names
        self._setup_schemas()

    def _setup_schemas(self) -> None:
        """Register necessary schemas with the config service."""
        workflow_specs_key = ConfigKey(
            service_name='data_reduction', key='workflow_specs'
        )

        self._config_service.register_schema(workflow_specs_key, WorkflowSpecs)
        self._config_service.register_schema(
            _persistent_configs_key, PersistentWorkflowConfigs
        )

        for source_name in self._source_names:
            workflow_status_key = ConfigKey(
                source_name=source_name,
                service_name='data_reduction',
                key='workflow_status',
            )
            workflow_config_key = ConfigKey(
                source_name=source_name,
                service_name="data_reduction",
                key="workflow_config",
            )

            self._config_service.register_schema(workflow_status_key, WorkflowStatus)
            self._config_service.register_schema(workflow_config_key, WorkflowConfig)

    def get_workflow_specs(self) -> WorkflowSpecs:
        """Get current workflow specifications."""
        workflow_specs_key = ConfigKey(
            service_name='data_reduction', key='workflow_specs'
        )
        return self._config_service.get(workflow_specs_key, WorkflowSpecs())

    def get_persistent_configs(self) -> PersistentWorkflowConfigs:
        """Get persistent workflow configurations."""
        return self._config_service.get(
            _persistent_configs_key, PersistentWorkflowConfigs()
        )

    def save_persistent_configs(self, configs: PersistentWorkflowConfigs) -> None:
        """Save persistent workflow configurations."""
        self._config_service.update_config(_persistent_configs_key, configs)

    def send_workflow_config(self, source_name: str, config: WorkflowConfig) -> None:
        """Send workflow configuration to a source."""
        config_key = ConfigKey(
            source_name=source_name,
            service_name="data_reduction",
            key="workflow_config",
        )
        self._config_service.update_config(config_key, config)

    def subscribe_to_workflow_specs(self, callback: callable) -> None:
        """Subscribe to workflow specs updates."""
        workflow_specs_key = ConfigKey(
            service_name='data_reduction', key='workflow_specs'
        )
        self._config_service.subscribe(workflow_specs_key, callback)

    def subscribe_to_workflow_status(
        self, source_name: str, callback: callable
    ) -> None:
        """Subscribe to workflow status updates for a source."""
        workflow_status_key = ConfigKey(
            source_name=source_name,
            service_name='data_reduction',
            key='workflow_status',
        )
        self._config_service.subscribe(workflow_status_key, callback)


class WorkflowController(WorkflowControllerBase):
    """
    Workflow controller backed by a config service.

    This controller manages workflow operations by interacting with a config service
    for starting/stopping workflows and maintaining local state for tracking.
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
        self.no_selection = object()

        # Initialize all sources with UNKNOWN status
        self._workflow_status: dict[str, WorkflowStatus] = {
            source_name: WorkflowStatus(source_name=source_name)
            for source_name in self._source_names
        }

        self._workflow_specs: WorkflowSpecs = WorkflowSpecs()

        # Callbacks
        self._workflow_specs_callbacks: list[callable] = []
        self._workflow_status_callbacks: list[callable] = []

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
        self._service.subscribe_to_workflow_specs(self._on_workflow_specs_updated)

        # Subscribe to workflow status for each source
        for source_name in self._source_names:
            self._service.subscribe_to_workflow_status(
                source_name, self._on_workflow_status_updated
            )

    def _on_workflow_specs_updated(self, workflow_specs: WorkflowSpecs) -> None:
        """Handle workflow specs updates from service."""
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
                callback()
            except Exception as e:  # noqa: PERF203
                self._logger.error('Error in workflow specs update callback: %s', e)

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

    def _on_workflow_status_updated(self, status: WorkflowStatus) -> None:
        """Handle workflow status updates from service."""
        self._logger.info('Received workflow status update: %s', status)
        self._workflow_status[status.source_name] = status
        for callback in self._workflow_status_callbacks:
            self._notify_workflow_status_update(callback)

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

    def stop_workflow_for_source(self, source_name: str) -> None:
        """Stop a running workflow for a specific source."""
        self._logger.info('Stopping workflow for source %s', source_name)

        # Send None to stop the workflow
        self._service.send_workflow_config(source_name, WorkflowConfig(identifier=None))
        self._on_workflow_status_updated(
            WorkflowStatus(source_name=source_name, status=WorkflowStatusType.STOPPING)
        )

    def remove_workflow_for_source(self, source_name: str) -> None:
        """Remove a stopped workflow from tracking."""
        self._logger.info('Removing workflow for source %s', source_name)
        # Reset status to UNKNOWN (back to initial state)
        self._on_workflow_status_updated(WorkflowStatus(source_name=source_name))

    def get_all_workflow_status(self) -> dict[str, WorkflowStatus]:
        """Get workflow status for all tracked sources."""
        return self._workflow_status.copy()

    def get_workflow_spec(self, workflow_id: WorkflowId) -> WorkflowSpec | None:
        """Get the current workflow specification for the given Id."""
        return self._workflow_specs.workflows.get(workflow_id)

    def subscribe_to_workflow_updates(self, callback: callable) -> None:
        """Subscribe to workflow updates."""
        self._workflow_specs_callbacks.append(callback)

    def _notify_workflow_status_update(self, callback: callable):
        try:
            callback()
        except Exception as e:
            self._logger.error('Error in workflow status update callback: %s', e)

    def subscribe_to_workflow_status_updates(self, callback: callable) -> None:
        """Subscribe to workflow status updates."""
        self._workflow_status_callbacks.append(callback)
        self._notify_workflow_status_update(callback)

    def load_workflow_config(
        self, workflow_id: WorkflowId
    ) -> PersistentWorkflowConfig | None:
        """Load saved workflow configuration."""
        all_configs = self._service.get_persistent_configs()
        return all_configs.configs.get(workflow_id)

    def get_workflow_name(self, workflow_id: WorkflowId | None) -> str:
        """Get workflow name from ID, fallback to ID if not found."""
        if workflow_id is None:
            return "None"
        if (spec := self.get_workflow_spec(workflow_id)) is not None:
            return spec.name
        return str(workflow_id)

    def get_status_display_info(self, status: WorkflowStatus) -> dict[str, str]:
        """Get display information for a workflow status."""
        if status.status == WorkflowStatusType.STARTING:
            return {
                'color': '#ffc107',  # Yellow
                'text': 'Starting...',
                'button_name': 'Stop',
                'button_type': 'primary',
                'opacity_style': '',
            }
        elif status.status == WorkflowStatusType.RUNNING:
            return {
                'color': '#28a745',  # Green
                'text': 'Running',
                'button_name': 'Stop',
                'button_type': 'primary',
                'opacity_style': '',
            }
        elif status.status == WorkflowStatusType.STOPPING:
            return {
                'color': '#b87817',  # Orange
                'text': 'Stopping...',
                'button_name': 'Stop',
                'button_type': 'primary',
                'opacity_style': '',
            }
        elif status.status == WorkflowStatusType.STARTUP_ERROR:
            return {
                'color': '#dc3545',  # Red
                'text': 'Error',
                'button_name': 'Remove',
                'button_type': 'light',
                'opacity_style': 'opacity: 0.7;',
            }
        elif status.status == WorkflowStatusType.STOPPED:
            return {
                'color': '#6c757d',  # Gray
                'text': 'Stopped',
                'button_name': 'Remove',
                'button_type': 'light',
                'opacity_style': 'opacity: 0.7;',
            }
        else:  # UNKNOWN
            return {
                'color': '#6c757d',  # Gray
                'text': 'Unknown',
                'button_name': 'Remove',
                'button_type': 'light',
                'opacity_style': 'opacity: 0.7;',
            }

    def is_no_selection(self, value: WorkflowId | object) -> bool:
        """Check if the given value represents no workflow selection."""
        return value is self.no_selection

    def get_default_workflow_selection(self) -> object:
        """Get the default value for no workflow selection."""
        return self.no_selection

    def get_workflow_options(self) -> dict[str, WorkflowId | object]:
        """Get workflow options for selector widget."""
        select_text = "--- Click to select a workflow ---"
        options = {select_text: self.get_default_workflow_selection()}
        options.update(
            {
                spec.name: workflow_id
                for workflow_id, spec in self._workflow_specs.workflows.items()
            }
        )
        return options

    def workflow_exists(self, workflow_id: WorkflowId) -> bool:
        """Check if a workflow ID exists in current specs."""
        return workflow_id in self._workflow_specs.workflows

    def get_initial_parameter_values(self, workflow_id: WorkflowId) -> dict[str, Any]:
        """Get initial parameter values for a workflow."""
        if (workflow_spec := self.get_workflow_spec(workflow_id)) is None:
            return {}
        values = {}

        # Start with defaults
        for param in workflow_spec.parameters:
            values[param.name] = param.default

        # Override with persistent config if available
        persistent_config = self.load_workflow_config(workflow_id)
        if persistent_config:
            values.update(persistent_config.config.values)

        return values

    def get_initial_source_names(self, workflow_id: WorkflowId) -> list[str]:
        """Get initial source names for a workflow."""
        persistent_config = self.load_workflow_config(workflow_id)
        if persistent_config:
            return persistent_config.source_names
        return []

    def get_workflow_description(self, workflow_id: WorkflowId | object) -> str | None:
        """
        Get the description for a workflow ID or selection value.

        Returns `None` if no workflow is selected.
        """
        if (spec := self.get_workflow_spec(workflow_id)) is None:
            return None
        return spec.description
