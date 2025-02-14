# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Protocol

from ..config.raw_detectors import available_instruments
from .processor import Processor


class ServiceBase(ABC):
    def __init__(self, *, name: str | None = None, log_level: int = logging.INFO):
        self._logger = logging.getLogger(name or __name__)
        self._setup_logging(log_level)
        self._running = False
        self._setup_signal_handlers()

    @staticmethod
    def configure_logging(log_level: int) -> None:
        """Configure logging for the root logger if not already configured"""
        root = logging.getLogger()
        if not root.handlers:  # Only configure if no handlers exist
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                stream=sys.stdout,
            )

    def _setup_logging(self, log_level: int) -> None:
        """Configure logging for this service instance if not already configured"""
        self.configure_logging(log_level)
        self._logger.setLevel(log_level)

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
        self._start_impl()
        self._logger.info("Service started")
        if blocking:
            self.run_forever()

    def stop(self) -> None:
        """Stop the service gracefully"""
        self._logger.info("Stopping service...")
        self._running = False
        self._stop_impl()
        self._logger.info("Service stopped")

    @abstractmethod
    def _start_impl(self) -> None:
        """Start the service implementation"""

    @abstractmethod
    def run_forever(self) -> None:
        """Block forever, waiting for signals"""

    @abstractmethod
    def _stop_impl(self) -> None:
        """Stop the service implementation"""


class StartStoppable(Protocol):
    def start(self) -> None: ...

    def stop(self) -> None: ...


class Service(ServiceBase):
    """
    Complete service with proper lifecycle management.

    Calls the injected processor in a loop with a configurable poll interval.
    """

    def __init__(
        self,
        *,
        children: list[StartStoppable] | None = None,
        processor: Processor,
        name: str | None = None,
        log_level: int = logging.INFO,
        poll_interval: float = 0.01,
    ):
        super().__init__(name=name, log_level=log_level)
        self._poll_interval = poll_interval
        self._children = children or []
        self._processor = processor
        self._thread: threading.Thread | None = None

    def _start_impl(self) -> None:
        """Start the service and block until stopped"""
        for child in self._children:
            child.start()
        self._thread = threading.Thread(target=self._run_loop)
        self._thread.start()

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
                start_time = time.monotonic()
                self._processor.process()
                elapsed = time.monotonic() - start_time
                remaining = max(0.0, self._poll_interval - elapsed)
                if remaining > 0:
                    time.sleep(remaining)
        except RuntimeError:
            self._logger.exception("Error in service loop")
            self.stop()
        finally:
            self._logger.info("Service loop stopped")

    def _stop_impl(self) -> None:
        """Stop the service gracefully"""
        if self._thread:
            self._thread.join()
        for child in self._children:
            child.stop()

    @staticmethod
    def setup_arg_parser(description: str) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            '--instrument',
            choices=available_instruments(),
            default='dummy',
            help='Select the instrument',
        )
        parser.add_argument(
            '--log-level',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='INFO',
            help='Set the logging level',
        )
        return parser


def get_env_defaults(
    *, parser: argparse.ArgumentParser, prefix: str = 'BEAMLIME'
) -> dict[str, Any]:
    """Get defaults from environment variables based on parser arguments."""
    env_defaults = {}
    for action in parser._actions:
        if action.dest == 'help':
            continue
        # Convert --arg-name to BEAMLIME_ARG_NAME
        env_name = f"{prefix}_{action.dest.upper().replace('-', '_')}"
        env_val = os.getenv(env_name)
        if env_val is not None:
            if isinstance(action.default, bool):
                env_defaults[action.dest] = env_val.lower() in ('true', '1', 'yes')
            elif isinstance(action.default, int):
                env_defaults[action.dest] = int(env_val)
            else:
                env_defaults[action.dest] = env_val
    return env_defaults
