# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import param

from ..config import models
from .params import MonitorDataParam


class TOAEdgesParam(MonitorDataParam):
    low = param.Number(
        default=0.0, doc="Lower bound of the time window in microseconds."
    )
    high = param.Number(
        default=72_000.0, doc="Upper bound of the time window in microseconds."
    )
    num_edges = param.Integer(
        default=100,
        bounds=(1, 1000),
        doc="Number of edges to use for the time of arrival histogram.",
    )
    unit = param.Selector(
        default='us',
        objects=['ns', 'us', 'ms', 's'],
        doc="Physical unit for time values.",
    )

    @property
    def config_key_name(self) -> str:
        return 'toa_edges'

    @property
    def schema(self) -> type[models.TOAEdges]:
        return models.TOAEdges

    def panel(self):
        return self.param.num_edges
