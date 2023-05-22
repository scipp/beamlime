# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import asyncio
from logging import DEBUG, ERROR, INFO, WARN, Logger
from typing import Any, Callable, Literal, Union, final

from ..communication.broker import CommunicationBroker
from ..core.schedulers import async_retry
from ..logging.loggers import BeamlimeLogger


class LogMixin:
    """
    Logging interfaces.
    Mixin assumes the inheriting class meets the ``BeamlimeApplicationProtocol``.

    Protocol
    --------
    BeamlimeLoggingProtocol
    """

    _logger = None

    @property
    def logger(self) -> Logger:
        return self._logger

    def set_logger(self, logger: Union[Logger, None]) -> None:
        """Set self logger as ``beamlime`` logger if not provided."""
        if isinstance(logger, Logger):
            self._logger = logger
        else:
            from ..logging import get_logger

            self._logger = get_logger()

    def _log(self, level: int, msg: str, args: tuple):
        if isinstance(self.logger, BeamlimeLogger):
            self.logger._log(level=level, msg=msg, args=args, app_name=self.app_name)
        elif isinstance(self.logger, Logger):
            if not self.logger.isEnabledFor(level):
                return
            from ..logging.formatters import EXTERNAL_MESSAGE_HEADERS

            self.logger._log(
                level=level,
                msg=EXTERNAL_MESSAGE_HEADERS.fmt % (self.app_name, msg),
                args=args,
                extra={"app_name": self.app_name},
            )
        else:
            raise ValueError(
                "`logger` should be an instance of `logging.Logger` "
                "or `beamlime.logging.BeamlimeLogger`."
            )

    def debug(self, msg: str, *args) -> None:
        self._log(level=DEBUG, msg=msg, args=args)

    def info(self, msg: str, *args) -> None:
        self._log(level=INFO, msg=msg, args=args)

    def warning(self, msg: str, *args) -> None:
        self._log(level=WARN, msg=msg, args=args)

    def error(self, msg: str, *args) -> None:
        self._log(level=ERROR, msg=msg, args=args)


class ApplicationPausedException(Exception):
    ...


class ApplicationNotPausedException(Exception):
    ...


class ApplicationStartedException(Exception):
    ...


class ApplicationNotStartedException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__("Application not started", *args)


class FlagControlMixin:
    """
    Process control interfaces.
    Mixin assumes the inheriting class meets the ``BeamlimeApplicationProtocol``.

    Protocol
    --------
    BeamlimeApplicationControlProtocol
    """

    _started = False
    _paused = True

    def start(self) -> None:
        self.info("Control command 'start' received.")
        if self._started:
            raise ApplicationStartedException("Application already started.")
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
        if not self._started:
            raise ApplicationNotStartedException
        elif self._paused:
            raise ApplicationPausedException("Application already paused.")
        self._paused = True
        self.debug("Flag updated, paused flag: %s", self._paused)

    def resume(self) -> None:
        self.info("Control command 'resume' received.")
        if not self._started:
            raise ApplicationNotStartedException
        elif not self._paused:
            raise ApplicationNotPausedException("Application is already running.")
        self._paused = False
        self.debug("Flag updated, paused flag: %s", self._paused)

    def stop(self) -> None:
        self.info("Control command 'stop' received.")
        if not self._started:
            raise ApplicationNotStartedException
        elif not self._paused:
            self.warning(
                "Control command `stop` was called with the application running, "
                "trying control command `pause` first ..."
            )
            self.pause()
        self._started = False
        self.debug("Flag updated, started flag: %s", self._started)


class CoroutineMixin:
    """
    Application coroutine interfaces.
    Mixin assumes the inheriting class meets the ``BeamlimeApplicationProtocol``.

    Protocol
    --------
    BeamlimeCoroutineProtocol

    Examples
    --------
    ```
    class DownStreamApp(BeamlimeApplicationInterface):
        timeout = 1
        wait_interval = 0.1

        async def _run(self):
            # prepare process
            delivered = await self.put("Start Message")
            first_msg_timeout = 30  # s
            if await self.get(timeout=first_msg_timeout) == "Expected Message":
                new_data = await self.get()

            while should_proceed() and delivered and new_data:
                result = await self.process()
                delivered = await self.put(result)
                new_data = await self.get()

            await self.put("Stop Message")
            self.info("Task completed.")

    app = DownStreamApp()
    app.run()  # should start coroutine ``_run``, which
               # receives data, processes and sends the result every 0.1 second.
    # or if there is another coroutine running,
    app.create_task()
    ```
    """

    @final
    async def _should_do(
        self, checker: Callable, *exceptions, wait_on_true: bool = False
    ) -> bool:
        _check = async_retry(
            *exceptions,
            max_trials=int(self.timeout / self.wait_interval),
            interval=self.wait_interval,
        )(checker)

        try:
            if (result := await _check()) and wait_on_true:
                await asyncio.sleep(self.wait_interval)
            return result
        except exceptions:
            return False

    async def should_start(self, wait_on_true: bool = False) -> bool:
        waited_error = ApplicationNotStartedException

        async def _is_app_started() -> Literal[True]:
            if not self._started:
                self.debug("Application not started. Waiting for ``start`` command...")
                raise waited_error
            return True

        return await self._should_do(
            _is_app_started, waited_error, wait_on_true=wait_on_true
        )

    async def should_proceed(self, wait_on_true: bool = False) -> bool:
        waited_error = ApplicationPausedException

        async def _is_app_resumed() -> bool:
            if not self._started:
                self.debug("Application stopped. Should not proceed the loop.")
                return False
            elif self._paused:
                self.debug("Application paused. Waiting for ``resume`` command...")
                raise waited_error
            return True

        return await self._should_do(
            _is_app_resumed, waited_error, wait_on_true=wait_on_true
        )

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


class BrokerBasedCommunicationMixin:
    """
    Communication Interfaces

    Protocol
    --------
    BeamlimeCommunicationProtocol

    """

    _broker = None

    @property
    def broker(self) -> CommunicationBroker:
        return self._broker

    @broker.setter
    def broker(self, _broker: CommunicationBroker) -> None:
        self._broker = _broker

    async def get(
        self,
        *args,
        channel: str = None,
        timeout: float = None,
        wait_interval: float = None,
        **kwargs,
    ) -> Any:
        return await self.broker.get(
            *args,
            app_name=self.app_name,
            channel=channel,
            timeout=timeout or self.timeout,
            wait_interval=wait_interval or self.wait_interval,
            **kwargs,
        )

    async def put(
        self,
        data: Any,
        *args,
        channel: str = None,
        timeout: float = None,
        wait_interval: float = None,
        **kwargs,
    ) -> Any:
        return await self.broker.put(
            data,
            *args,
            app_name=self.app_name,
            channel=channel,
            timeout=timeout or self.timeout,
            wait_interval=wait_interval or self.wait_interval,
            **kwargs,
        )

    async def consume(
        self,
        *args,
        channel: str = None,
        chunk_size: int = 1,
        timeout: float = None,
        wait_interval: float = None,
        **kwargs,
    ) -> Any:
        return await self.broker.consume(
            *args,
            app_name=self.app_name,
            channel=channel,
            timeout=timeout or self.timeout,
            wait_interval=wait_interval or self.wait_interval,
            chunk_size=chunk_size,
            **kwargs,
        )

    async def produce(
        self,
        data: Any,
        *args,
        channel: str = None,
        key: str,
        timeout: float = None,
        wait_interval: float = None,
        **kwargs,
    ) -> Any:
        return await self.broker.produce(
            data,
            *args,
            app_name=self.app_name,
            channel=channel,
            timeout=timeout or self.timeout,
            wait_interval=wait_interval or self.wait_interval,
            key=key,
            **kwargs,
        )
