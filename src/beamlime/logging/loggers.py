# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import logging
import sys
from contextlib import contextmanager
from typing import Union

from .sources import find_caller


@contextmanager
def hold_logging() -> None:
    from . import _lock

    _lock.acquire() if _lock else ...
    try:
        yield _lock
    finally:
        _lock.release() if _lock else ...


class BeamlimeLogger(logging.Logger):
    """
    Logger for beamlime application.

    Notes
    -----
    ``_logRecordFactory`` should be ``BeamlimeColorLogRecord``
    since it is the lowest LogRecord class in the inheritance hierarchy.
    It may be replaced with another class if we want to add more fields.
    """

    def __init__(self, name: str, level: Union[int, str] = 0) -> None:
        from .records import BeamlimeColorLogRecord

        self._logRecordFactory = BeamlimeColorLogRecord
        super().__init__(name, level)

    def _wrap_stack_info(
        self, stack_info: bool = False, stacklevel: int = 1
    ) -> tuple[str, int, str, Union[str, None]]:
        """Returns fn, lno, func, sinfo"""
        try:
            return find_caller(stack_info, stacklevel)
        except ValueError:
            return "(unknown file)", 0, "(unknown function)", None

    def _wrap_exception_info(
        self, exc_info: Union[tuple, None] = None
    ) -> Union[tuple, None]:
        if isinstance(exc_info, BaseException):
            return (type(exc_info), exc_info, exc_info.__traceback__)
        elif exc_info and not isinstance(exc_info, tuple):
            return sys.exc_info()
        return exc_info

    def _log(
        self,
        level: int,
        msg: str,
        args: tuple,
        exc_info=None,
        extra=None,
        stack_info=False,
        stacklevel=1,
        app_name: str = "",
        **kwargs,
    ) -> None:
        """
        Low-level logging routine which creates a LogRecord and then calls
        all the handlers of this logger to handle the record.

        Overwrites ``logging.Logger._log``.

        """
        if not self.isEnabledFor(level):
            # level test is usually done by higher level log functions
            # But ``BeamlimeLogger._log`` is the only exposed interface for now.
            return

        exception_info = self._wrap_exception_info(exc_info=exc_info)
        fn, lno, func, sinfo = self._wrap_stack_info(
            stack_info=stack_info, stacklevel=stacklevel
        )
        extra_info = {} if extra is None else extra
        record = self._logRecordFactory(
            name=self.name,
            level=level,
            msg=str(msg),
            args=args,
            pathname=fn,
            lineno=lno,
            func=func,
            sinfo=sinfo,
            exc_info=exception_info,
            app_name=app_name,
            **kwargs,
        )
        for key in extra_info:
            if (key in ["message", "asctime"]) or (key in record.__dict__):
                raise KeyError("Attempt to overwrite %r in LogRecord" % key)

        record.__dict__.update(extra_info)
        self.handle(record)

    def addHandler(self, hdlr: logging.Handler) -> None:
        """
        Add a handler to this logger after checking if the format of the handler
        is matching with the ``self._logRecordFactory``.

        """
        with hold_logging():
            try:
                _ = hdlr.format(self._logRecordFactory(name="", level=0, msg=""))
            except KeyError as key_err:
                raise KeyError(
                    f"Handler with type {hdlr.__name__} does not have "
                    "a matching formatter"
                    f" with the LogRecord type {self._logRecordFactory}.\n",
                    key_err,
                )

            if not (hdlr in self.handlers):
                self.handlers.append(hdlr)
