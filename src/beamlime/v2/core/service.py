# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
import signal
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
        logger: logging.Logger | None = None,
        config: Config,
        processor: Processor,
    ):
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._processor = processor
        self._thread: threading.Thread | None = None
        self._running = False
        self._setup_signal_handlers()

    @property
    def is_running(self) -> bool:
        return self._running

    def _setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, _: Any) -> None:
        self._logger.info("Received signal %d, initiating shutdown...", signum)
        self.stop()

    def start(self) -> None:
        self._logger.info("Starting service...")
        self._running = True
        self._thread = threading.Thread(target=self._run_loop)
        self._thread.start()

    def _run_loop(self) -> None:
        try:
            while self.is_running:
                self._processor.process()
                time.sleep(self._config.get("service.poll_interval", 0.1))
        except KeyboardInterrupt:
            pass
        finally:
            self._logger.info("Loop stopped")

    def stop(self) -> None:
        self._logger.info("Stopping service...")
        self._running = False
        if self._thread:
            self._thread.join()
        self._logger.info("Service stopped")
