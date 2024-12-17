# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import threading
from typing import Any

from .config_subscriber import ConfigSubscriber
from .handler import ConfigProxy


class ConfigManager:
    """Manages configuration and ConfigSubscriber lifecycle"""

    def __init__(
        self,
        bootstrap_servers: str,
        service_name: str | None = None,
        initial_config: dict[str, Any] | None = None,
    ):
        self._config = initial_config or {}
        self._subscriber = ConfigSubscriber(
            bootstrap_servers=bootstrap_servers,
            service_name=service_name,
        )
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the config subscriber in a background thread"""
        self._thread = threading.Thread(target=self._subscriber.start)
        self._thread.daemon = True  # Thread will be terminated when main thread exits
        self._thread.start()

    def stop(self) -> None:
        """Stop the config subscriber"""
        if self._thread is not None:
            self._subscriber.stop()
            self._thread.join()
            self._thread = None

    def get(self, key: str, default: Any = None) -> Any:
        """Get config value, checking both static and dynamic configs"""
        return self._subscriber.get(key) or self._config.get(key, default)

    def proxy(self, namespace: str) -> ConfigProxy:
        """Create a proxy for accessing configuration with a namespace"""
        return ConfigProxy(self, namespace=namespace)
