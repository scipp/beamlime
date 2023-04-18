# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)

import logging
import os
import sys
from typing import Any, Callable, Optional, TypeVar, Union

from .sources import is_file_logging_source, retrieve_current_frame

frame = TypeVar("frame")


def hold_logging(logging_func: Callable) -> Callable:
    from functools import wraps
    from inspect import signature

    @wraps(logging_func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        from . import _lock

        if _lock:
            _lock.acquire()

        try:
            out = logging_func(*args, **kwargs)
        except Exception as e:
            if _lock:
                _lock.release()
            raise (e)
        finally:
            if _lock:
                _lock.release()

        return out

    wrapper.__signature__ = signature(logging_func)
    return wrapper


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

    def find_caller(self, stack_info=False, stacklevel=1):
        """
        Find the stack frame of the caller so that we can note the source
        file name, line number and function name.

        Copied from ``logging.Logger.findCaller``.
        """
        import io
        import traceback

        def _get_last_frame(frm: frame, stack_level: int):
            if frm and (stack_level > 1):
                return _get_last_frame(frm.f_back, stack_level - 1)
            return frm

        def _retrieve_caller_info(
            frm: frame, stack_info: bool
        ) -> tuple[str, int, str, Optional[str]]:
            if not hasattr(frm, "f_code"):
                return "(unknown file)", 0, "(unknown function)", None
            elif is_file_logging_source(os.path.normcase(frm.f_code.co_filename)):
                return _retrieve_caller_info(frm.f_back, stack_info=stack_info)
            else:
                if not stack_info:
                    return (
                        frm.f_code.co_filename,
                        frm.f_lineno,
                        frm.f_code.co_name,
                        None,
                    )
                else:
                    sio = io.StringIO()
                    sio.write("Stack (most recent call last):\n")
                    traceback.print_stack(frm, file=sio)
                    sinfo = sio.getvalue().removesuffix("\n")
                    sio.close()
                    return (
                        frm.f_code.co_filename,
                        frm.f_lineno,
                        frm.f_code.co_name,
                        sinfo,
                    )

        # On some versions of IronPython, currentframe() returns None if
        # IronPython isn't run with -X:Frames.
        orig_f = retrieve_current_frame()
        _frame = (
            orig_f if orig_f is None else _get_last_frame(orig_f.f_back, stacklevel)
        )
        return _retrieve_caller_info(_frame, stack_info=stack_info)

    def _wrap_stack_info(
        self, stack_info: bool = False, stacklevel: int = 1
    ) -> tuple[str, int, str, Union[str, None]]:
        try:
            fn, lno, func, sinfo = self.find_caller(stack_info, stacklevel)
            return fn, lno, func, sinfo
        except ValueError:
            ...
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
            msg=msg,
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

    @hold_logging
    def addHandler(self, hdlr: logging.Handler) -> None:
        """
        Add a handler to this logger after checking if the format of the handler
        is matching with the ``self._logRecordFactory``.

        """
        try:
            _ = hdlr.format(self._logRecordFactory(name="", level=0, msg=""))
        except KeyError as key_err:
            raise KeyError(
                f"Handler with type {hdlr.__name__} does not have "
                "a matching formatter"
                f" with the LogRecord type {self._logRecordFactory}.\n",
                key_err,
            )

        try:
            if not (hdlr in self.handlers):
                self.handlers.append(hdlr)
        finally:
            pass
