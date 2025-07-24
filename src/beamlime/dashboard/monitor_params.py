# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import param

from ..config import models
from .config_backed_param import MonitorDataParam


class TOAEdgesParam(MonitorDataParam):
    low = param.Number(
        default=0.0, doc="Lower bound of the time window in microseconds."
    )
    high = param.Number(
        default=1000.0 / 14, doc="Upper bound of the time window in microseconds."
    )
    num_bins = param.Integer(
        default=500,
        bounds=(1, 2000),
        doc="Number of bins to use for the time of arrival histogram.",
    )
    unit = param.Selector(
        default='ms',
        objects=['ms'],
        doc="Physical unit for time values.",
    )

    @property
    def config_key_name(self) -> str:
        return 'toa_edges'

    @property
    def schema(self) -> type[models.TOAEdges]:
        return models.TOAEdges

    def panel(self):
        return self
