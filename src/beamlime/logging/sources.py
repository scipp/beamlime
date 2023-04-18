# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# See comments in ``logging.Logger._src_file``
# for explanation why we didn't use __file__

import logging
import os
import sys

_logging_src_file = os.path.normcase(logging.addLevelName.__code__.co_filename)


def is_file_general_logging_source(filepath: str) -> bool:
    # This function didn't really have to be separate from``is_file_loggin_source``
    # but in order to use ``__code__.co_filename``, we needed another function.
    return filepath == _logging_src_file


_bm_logging_src_dir = os.path.normcase(
    os.path.dirname(is_file_general_logging_source.__code__.co_filename)
)


def is_file_logging_source(filepath: str) -> bool:
    return (
        is_file_general_logging_source(filepath)
        or os.path.dirname(__file__) == _bm_logging_src_dir
    )


if hasattr(sys, "_getframe"):

    def retrieve_current_frame():
        return sys._getframe(3)

else:

    def retrieve_current_frame():
        """Return the frame object for the caller's stack frame."""
        try:
            raise Exception
        except Exception:
            return sys.exc_info()[2].tb_frame.f_back
