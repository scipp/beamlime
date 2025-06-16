# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from collections.abc import Callable

import param


class ParamUpdaterMixin:
    """Mixin to provide a method for updating parameters."""

    def param_updater(self) -> Callable[..., None]:
        """Wrapper to make linters/mypy happy with the callback signature."""

        def update_callback(**kwargs) -> None:
            self.param.update(**kwargs)

        return update_callback


class TOARangeParam(param.Parameterized, ParamUpdaterMixin):
    enabled = param.Boolean(
        default=False, doc="Enable the time of arrival range filter."
    )
    low = param.Number(
        default=0.0, doc="Lower bound of the time window in microseconds."
    )
    high = param.Number(
        default=72_000.0, doc="Upper bound of the time window in microseconds."
    )
    unit = param.Selector(
        default='us',
        objects=['ns', 'us', 'ms', 's'],
        doc="Physical unit for time values.",
    )
