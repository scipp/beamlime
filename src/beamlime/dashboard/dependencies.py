"""Dashboard dependency injection."""

import logging

from .models import DashboardConfig
from .presentation.callbacks import CallbackController
from .services.configuration import ConfigurationService
from .services.figures import FigureService
from .services.orchestrator import DashboardOrchestrator

logger = logging.getLogger(__name__)


class DashboardDependencies:
    """Dependency container for dashboard components."""

    def __init__(self, config: DashboardConfig):
        self.config = config

        # Service layer
        self.config_service = ConfigurationService()
        self.figure_service = FigureService()
        self.orchestrator = DashboardOrchestrator(
            config_service=self.config_service, figure_service=self.figure_service
        )

        # Presentation layer
        self.callback_controller = CallbackController(orchestrator=self.orchestrator)

        logger.info('Dashboard dependencies initialized')
