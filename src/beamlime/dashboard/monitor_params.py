# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import param

from ..config import models
from beamlime.config import keys
from beamlime.config.schema_registry import ConfigItemSpec
from .config_backed_param import MonitorDataParam


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
    def spec(self) -> ConfigItemSpec:
        return keys.MONITOR_TOA_EDGES

    def panel(self):
        return self.param.num_edges
