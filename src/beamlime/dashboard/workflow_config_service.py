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

import beamlime.config.keys as keys
from beamlime.config.workflow_spec import (
    PersistentWorkflowConfigs,
    WorkflowConfig,
    WorkflowStatus,
)

from .config_service import ConfigService


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

    def subscribe_to_workflow_status(
        self, source_name: str, callback: Callable[[WorkflowStatus], None]
    ) -> None:
        """Subscribe to workflow status updates for a source."""
        ...


class ConfigServiceAdapter(WorkflowConfigService):
    """
    Adapter to make ConfigService compatible with WorkflowConfigService protocol.
    """

    def __init__(
        self,
        config_service: ConfigService,
        source_names: list[str],
        backend_service_name: str = 'data_reduction',
    ):
        self._config_service = config_service
        self._source_names = source_names
        # This is a hack, which we could avoid if we unify the backend services and
        # remove the service-name-based config message filtering. It is currently
        # unclear whether this will happen.
        match backend_service_name:
            case 'data_reduction':
                self._CONFIG = keys.WORKFLOW_CONFIG
                self._STATUS = keys.WORKFLOW_STATUS
            case 'monitor_data':
                self._CONFIG = keys.MONITOR_WORKFLOW_CONFIG
                self._STATUS = keys.MONITOR_WORKFLOW_STATUS
            case _:
                raise ValueError(
                    f"Unknown backend service name: {backend_service_name}"
                )

    def get_persistent_configs(self) -> PersistentWorkflowConfigs:
        """Get persistent workflow configurations."""
        return self._config_service.get_config(
            keys.PERSISTENT_WORKFLOW_CONFIGS.create_key(), PersistentWorkflowConfigs()
        )

    def save_persistent_configs(self, configs: PersistentWorkflowConfigs) -> None:
        """Save persistent workflow configurations."""
        self._config_service.update_config(
            keys.PERSISTENT_WORKFLOW_CONFIGS.create_key(), configs
        )

    def send_workflow_config(self, source_name: str, config: WorkflowConfig) -> None:
        """Send workflow configuration to a source."""
        config_key = self._CONFIG.create_key(source_name=source_name)
        self._config_service.update_config(config_key, config)

    def subscribe_to_workflow_status(
        self, source_name: str, callback: Callable[[WorkflowStatus], None]
    ) -> None:
        """Subscribe to workflow status updates for a source."""
        workflow_status_key = self._STATUS.create_key(source_name=source_name)
        self._config_service.subscribe(workflow_status_key, callback)
