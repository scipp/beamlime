# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# See comments in ``logging.Logger._src_file``
# for explanation why we didn't use __file__

import logging
import os
import sys
from typing import Optional, TypeVar

_logging_src_file = os.path.normcase(logging.addLevelName.__code__.co_filename)


def is_file_general_logging_source(filepath: str) -> bool:
    # This function didn't really have to be separate from``is_file_loggin_source``
    # but in order to use ``__code__.co_filename``, we needed another function.
    return filepath == _logging_src_file


_bm_logging_src_dir = os.path.normcase(
    os.path.dirname(is_file_general_logging_source.__code__.co_filename)
)


def _is_file_logging_source(filepath: str) -> bool:
    return (
        is_file_general_logging_source(filepath)
        or os.path.dirname(__file__) == _bm_logging_src_dir
    )


if hasattr(sys, "_getframe"):

    def _retrieve_current_frame():
        return sys._getframe(3)

else:

    def _retrieve_current_frame():
        """Return the frame object for the caller's stack frame."""
        try:
            raise Exception
        except Exception:
            return sys.exc_info()[2].tb_frame.f_back


frame = TypeVar("frame")


def _get_last_frame(frm: frame, stack_level: int):
    if frm and (stack_level > 1):
        return _get_last_frame(frm.f_back, stack_level - 1)
    elif hasattr(frm, "f_code") and _is_file_logging_source(
        os.path.normcase(frm.f_code.co_filename)
    ):
        return _get_last_frame(frm.f_back, stack_level)
    return frm


def _retrieve_caller_info(
    frm: frame, stack_info: bool
) -> tuple[str, int, str, Optional[str]]:
    import io
    import traceback

    if not hasattr(frm, "f_code"):
        return "(unknown file)", 0, "(unknown function)", None
    elif not stack_info:
        frm_co = frm.f_code
        return (frm_co.co_filename, frm.f_lineno, frm_co.co_name, None)
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


def find_caller(self, stack_info=False, stacklevel=1):
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.

    Copied from ``logging.Logger.findCaller``.
    """
    # On some versions of IronPython, currentframe() returns None if
    # IronPython isn't run with -X:Frames.
    orig_f = _retrieve_current_frame()
    last_frame = (
        orig_f if orig_f is None else _get_last_frame(orig_f.f_back, stacklevel)
    )
    return _retrieve_caller_info(last_frame, stack_info=stack_info)
