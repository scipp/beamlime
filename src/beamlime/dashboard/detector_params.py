# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
import param

from beamlime.config import keys
from beamlime.config.schema_registry import ConfigItemSpec
from .config_backed_param import DetectorDataParam


class TOARangeParam(DetectorDataParam):
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

    @property
    def spec(self) -> ConfigItemSpec:
        return keys.DETECTOR_TOA_RANGE
