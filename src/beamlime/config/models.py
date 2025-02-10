# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
from typing import Literal

import scipp as sc
from pydantic import BaseModel, Field

TimeUnit = Literal['ns', 'us', 'ms', 's']


class TOARange(BaseModel):
    """Time of arrival range filter settings."""

    enabled: bool = Field(default=False, description="Enable the range filter.")
    low: float = Field(default=0.0, description="Lower bound of the time window.")
    high: float = Field(default=72_000.0, description="Upper bound of the time window.")
    unit: TimeUnit = Field(default="us", description="Physical unit for time values.")

    _low_ns: sc.Variable | None = None
    _high_ns: sc.Variable | None = None

    def model_post_init(self, /, __context) -> None:
        """Convert float values to scipp scalars after validation."""
        self._low_ns = sc.scalar(self.low, unit=self.unit).to(unit='ns')
        self._high_ns = sc.scalar(self.high, unit=self.unit).to(unit='ns')

    @property
    def low_ns(self) -> sc.Variable:
        """Low bound in nanoseconds as a scipp scalar."""
        return self._low_ns

    @property
    def high_ns(self) -> sc.Variable:
        """High bound in nanoseconds as a scipp scalar."""
        return self._high_ns
