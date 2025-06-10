"""Dashboard orchestrator service."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class DashboardOrchestrator:
    """Main coordinator for dashboard operations."""

    def __init__(
        self, config_service: 'ConfigurationService', figure_service: 'FigureService'
    ):
        self.config_service = config_service
        self.figure_service = figure_service
        logger.info('Dashboard orchestrator initialized')

    def get_current_figures(self) -> dict[str, Any]:
        """Get current plot figures."""
        return self.figure_service.create_all_figures()

    def update_configuration(self, **kwargs: Any) -> None:
        """Update processing configuration."""
        self.config_service.update_config(**kwargs)
        logger.info('Configuration updated via orchestrator')
