# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2023 Scipp contributors (https://github.com/scipp)
# Wrapper tools to write a decorator easier.
# See https://github.com/scipp/beamlime/discussions/46 for intended use-cases.

from functools import WRAPPER_ASSIGNMENTS, WRAPPER_UPDATES, update_wrapper
from inspect import signature
from typing import Callable


def wraps_better(
    wrapped: Callable, assigned=WRAPPER_ASSIGNMENTS, updated=WRAPPER_UPDATES
) -> Callable:
    """Update signature and __doc__ of decorated function from ``wrapped``."""

    def wrap_signature(wrapper: Callable) -> Callable:
        """Update signature and __doc__ of ``wrapper`` from ``wrapped``."""

        doc_wrapped = update_wrapper(
            wrapper, wrapped=wrapped, assigned=assigned, updated=updated
        )
        doc_wrapped.__signature__ = signature(wrapped)
        return wrapper

    return wrap_signature
