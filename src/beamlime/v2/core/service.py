# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from typing import Any

from .handler import Config
from .processor import Processor


class Service:
    """Complete service with proper lifecycle management"""

    def __init__(
        self,
        *,
        config: Config,
        processor: Processor,
        name: str | None = None,
        log_level: int = logging.INFO,
    ):
        self._setup_logging(log_level)
        self._logger = logging.getLogger(name or __name__)
        self._config = config
        self._processor = processor
        self._thread: threading.Thread | None = None
        self._running = False
        self._setup_signal_handlers()

    def _setup_logging(self, log_level: int) -> None:
        """Configure logging for the entire service"""
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            stream=sys.stdout,
        )
        logging.getLogger('beamlime').setLevel(log_level)

    @property
    def is_running(self) -> bool:
        return self._running

    def _setup_signal_handlers(self) -> None:
        """Setup handlers for graceful shutdown"""
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
        self._logger.info("Registered signal handlers")

    def _handle_shutdown(self, signum: int, _: Any) -> None:
        """Handle shutdown signals"""
        self._logger.info("Received signal %d, initiating shutdown...", signum)
        self.stop()
        sys.exit(0)

    def start(self, blocking: bool = True) -> None:
        """Start the service and block until stopped"""
        self._logger.info("Starting service...")
        self._running = True
        self._thread = threading.Thread(target=self._run_loop)
        self._thread.start()
        self._logger.info("Service started")
        if blocking:
            self.run_forever()

    def run_forever(self) -> None:
        """Block forever, waiting for signals"""
        while self.is_running:
            try:
                signal.pause()
            except KeyboardInterrupt:  # noqa: PERF203
                self.stop()

    def _run_loop(self) -> None:
        """Main service loop"""
        try:
            while self.is_running:
                self._processor.process()
                time.sleep(self._config.get("service.poll_interval", 0.1))
        except RuntimeError:
            self._logger.exception("Error in service loop")
            self.stop()
        finally:
            self._logger.info("Service loop stopped")

    def stop(self) -> None:
        """Stop the service gracefully"""
        self._logger.info("Stopping service...")
        self._running = False
        if self._thread:
            self._thread.join()
        self._logger.info("Service stopped")
