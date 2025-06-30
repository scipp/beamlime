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

from beamlime.config import keys
from beamlime.config.workflow_spec import (
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowSpecs,
    WorkflowStatus,
)
from beamlime.dashboard.config_service import ConfigService


class WorkflowConfigService(Protocol):
    """Protocol for workflow controller dependencies."""

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
    """
    Adapter to make ConfigService compatible with WorkflowConfigService protocol.

    This also registers necessary schemas for workflow management.
    """

    def __init__(self, config_service: ConfigService, source_names: list[str]):
        self._config_service = config_service
        self._source_names = source_names

    def get_persistent_configs(self) -> PersistentWorkflowConfigs:
        """Get persistent workflow configurations."""
        return self._config_service.get(
            keys.PERSISTENT_WORKFLOW_CONFIGS.create_key(), PersistentWorkflowConfigs()
        )

    def save_persistent_configs(self, configs: PersistentWorkflowConfigs) -> None:
        """Save persistent workflow configurations."""
        self._config_service.update_config(
            keys.PERSISTENT_WORKFLOW_CONFIGS.create_key(), configs
        )

    def send_workflow_config(self, source_name: str, config: WorkflowConfig) -> None:
        """Send workflow configuration to a source."""
        self._config_service.update_config(
            keys.WORKFLOW_CONFIG.create_key(source_name), config
        )

    def subscribe_to_workflow_specs(
        self, callback: Callable[[WorkflowSpecs], None]
    ) -> None:
        """Subscribe to workflow specs updates."""
        self._config_service.subscribe(keys.WORKFLOW_SPECS.create_key(), callback)

    def subscribe_to_workflow_status(
        self, source_name: str, callback: Callable[[WorkflowStatus], None]
    ) -> None:
        """Subscribe to workflow status updates for a source."""
        self._config_service.subscribe(
            keys.WORKFLOW_STATUS.create_key(source_name), callback
        )
