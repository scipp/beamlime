# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from logging import DEBUG, ERROR, INFO, WARN

from ..core.schedulers import async_timeout
from ..logging.loggers import BeamlimeLogger

MAX_INSTANCE_PAUSED = 20


class LogMixin:
    """
    Logging interfaces

    Protocol
    --------
    BeamlimeLoggingProtocol
    """

    def _log(self, level: int, msg: str, args: tuple):
        if isinstance(self.logger, BeamlimeLogger):
            self.logger._log(level=level, msg=msg, args=args, app_name=self.app_name)
        else:
            if not self.logger.isEnabledFor(level):
                return
            from ..logging.formatters import EXTERNAL_MESSAGE_HEADERS

            self.logger._log(
                level=level,
                msg=EXTERNAL_MESSAGE_HEADERS.fmt % (self.app_name, msg),
                args=args,
                extra={"app_name": self.app_name},
            )

    def debug(self, msg: str, *args) -> None:
        self._log(level=DEBUG, msg=msg, args=args)

    def info(self, msg: str, *args) -> None:
        self._log(level=INFO, msg=msg, args=args)

    def warning(self, msg: str, *args) -> None:
        self._log(level=WARN, msg=msg, args=args)

    def error(self, msg: str, *args) -> None:
        self._log(level=ERROR, msg=msg, args=args)


class FlagControlMixin:
    """
    Process control interfaces

    Protocol
    --------
    BeamlimeApplicationControlProtocol
    """

    _started = False
    _paused = True
    _stopped = False

    def start(self) -> None:
        self.info("Control command 'start' received.")
        if not self._started:
            self._started = True
            self._paused = False
            self.debug(
                "Flags updated, started flag: %s, paused flag: %s",
                self._started,
                self._paused,
            )

    def pause(self) -> None:
        self.info("Control command 'pause' received.")
        if not self._paused:
            self._paused = True
            self.debug("Flag updated, paused flag: %s", self._paused)

    def resume(self) -> None:
        self.info("Control command 'resume' received.")
        if not self._started:
            self.info("Application not started, trying control command 'start' ...")
            self.start()
        elif self._paused:
            self._paused = False
            self.debug("Flag updated, paused flag: %s", self._paused)

    def stop(self) -> None:
        self.info("Control command 'stop' received.")
        if not self._paused:
            self.info(
                "Application not paused, trying control command 'pause' first ..."
            )
            self.pause()
        if not self._stopped:
            self._stopped = True
            self.debug("Flag updated, stopped flag: %s", self._paused)


class ApplicationPausedException(Exception):
    ...


class CoroutineMixin:
    """
    Application coroutine interfaces.

    Protocol
    --------
    BeamlimeCoroutineProtocol

    Examples
    --------
    ```
    class DownStreamApp(BeamlimeApplicationInterface):
        _timeout = 1
        _wait_int = 0.1

        async def _run(self):
            # prepare process
            delivered = await self.send_data("Start Message.")
            new_data = await self.receive_data()
            while should_proceed() and delivered and new_data:
                if new_data == "Expected Start Message":
                    # do something
                result = await self.process()
                delivered = await self.send_data(result)
                new_data = await self.receive_data()
            self.info("Task completed")

    app = DownStreamApp()
    app.run()  # should start coroutine ``_run``, which
               # receives data, processes and sends the result every 0.1 second.
    # or if there is another coroutine running,
    app.create_task()
    ```
    """

    async def should_proceed(self):
        @async_timeout(ApplicationPausedException)
        async def wait_resumed(timeout: int, wait_interval: int):
            if not self._stopped and self._paused:
                self.debug("Application paused. Waiting for ``resume`` command...")
                raise ApplicationPausedException
            return not (self._stopped or self._paused)

        try:
            await asyncio.sleep(self._wait_int)
            return await wait_resumed(
                timeout=self._timeout, wait_interval=self._wait_int
            )
        except TimeoutError:
            self.stop()
            return False

    def run(self) -> None:
        """
        Run the application in a dependent event loop.

        """
        # TODO: ``run`` should also check if the communication-interface
        # is multi-process-friendly before running.
        # For example, if one of input_channel or output_channel is ``Queue``,
        # the application daemon should start by ``create_task`` not, ``run``.

        return asyncio.run(self._run())

    def create_task(self, /, name=None, context=None) -> asyncio.Task:
        """
        Start the application task in the currently running event loop.
        """
        if not hasattr(self, "_task") or self._task.done():
            # TODO: Remove try-except after updating the minimum python version to 3.11
            try:
                return asyncio.create_task(
                    self._run(), name=name, context=context
                )  # py311
            except TypeError:
                return asyncio.create_task(self._run(), name=name)  # py39, py310
