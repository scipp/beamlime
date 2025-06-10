"""Configuration management service."""

import logging
from typing import Any

from ..models import ProcessingConfig

logger = logging.getLogger(__name__)


class ConfigurationService:
    """Manages processing configuration state."""

    def __init__(self):
        self._config = ProcessingConfig()
        logger.info('Configuration service initialized')

    @property
    def config(self) -> ProcessingConfig:
        """Get current configuration."""
        return self._config

    def update_config(self, **kwargs: Any) -> None:
        """Update configuration with new values."""
        updated_data = self._config.model_dump()
        updated_data.update(kwargs)
        self._config = ProcessingConfig(**updated_data)
        logger.info('Configuration updated: %s', kwargs)

    def get_config_dict(self) -> dict[str, Any]:
        """Get configuration as dictionary."""
        return self._config.model_dump()
