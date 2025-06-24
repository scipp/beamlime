# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
"""
Provides a protocol and an adapter for managing workflow configurations.

This is not a real service but an adapter that wraps :py:class:`ConfigService` to
make it compatible with the :py:class:`WorkflowConfigService` protocol. This simplifies
the implementation and testing of :py:class:`WorkflowController`.
"""

from collections.abc import Callable
from typing import Protocol

from beamlime.config.models import ConfigKey
from beamlime.config.workflow_spec import (
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowSpecs,
    WorkflowStatus,
)

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

    def subscribe_to_workflow_specs(
        self, callback: Callable[[WorkflowSpecs], None]
    ) -> None:
        """Subscribe to workflow specs updates."""
        ...

    def subscribe_to_workflow_status(
        self, source_name: str, callback: Callable[[WorkflowStatus], None]
    ) -> None:
        """Subscribe to workflow status updates for a source."""
        ...


class ConfigServiceAdapter(WorkflowConfigService):
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

    def subscribe_to_workflow_specs(
        self, callback: Callable[[WorkflowSpecs], None]
    ) -> None:
        """Subscribe to workflow specs updates."""
        workflow_specs_key = ConfigKey(
            service_name='data_reduction', key='workflow_specs'
        )
        self._config_service.subscribe(workflow_specs_key, callback)

    def subscribe_to_workflow_status(
        self, source_name: str, callback: Callable[[WorkflowStatus], None]
    ) -> None:
        """Subscribe to workflow status updates for a source."""
        workflow_status_key = ConfigKey(
            source_name=source_name,
            service_name='data_reduction',
            key='workflow_status',
        )
        self._config_service.subscribe(workflow_status_key, callback)
