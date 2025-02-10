# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
from enum import Enum
from typing import Any, Literal

import scipp as sc
from pydantic import BaseModel, Field

TimeUnit = Literal['ns', 'us', 'ms', 's']


class TOARange(BaseModel):
    """Time of arrival range filter settings."""

    enabled: bool = Field(default=False, description="Enable the range filter.")
    low: float = Field(default=0.0, description="Lower bound of the time window.")
    high: float = Field(default=72_000.0, description="Upper bound of the time window.")
    unit: TimeUnit = Field(default="us", description="Physical unit for time values.")

    @property
    def range_ns(self) -> tuple[sc.Variable, sc.Variable] | None:
        """Time window range in nanoseconds as a scipp scalar."""
        if not self.enabled:
            return None
        return (
            sc.scalar(self.low, unit=self.unit).to(unit='ns'),
            sc.scalar(self.high, unit=self.unit).to(unit='ns'),
        )


class WeightingMethod(str, Enum):
    """
    Methods for pixel weighting.

    - PIXEL_NUMBER: Weight by the number of detector pixels contributing to each screen
        pixel.
    """

    PIXEL_NUMBER = 'pixel_number'


class PixelWeighting(BaseModel):
    """Settings for pixel weighting."""

    enabled: bool = Field(default=False, description="Enable pixel weighting.")
    method: WeightingMethod = Field(
        default=WeightingMethod.PIXEL_NUMBER, description="Method for pixel weighting."
    )


class StartTime(BaseModel):
    """Settings for the start time of the experiment or accumulation period."""

    value: float = Field(default=0, description="Start time.")
    unit: TimeUnit = Field(default="ns", description="Physical unit for time values.")

    _value_ns: int | None = None

    def model_post_init(self, /, __context: Any) -> None:
        """Perform relatively expensive operations after model initialization."""
        self._value_ns = int(sc.scalar(self.value, unit=self.unit).to(unit='ns').value)

    @property
    def value_ns(self) -> int:
        """Start time in nanoseconds as a scipp scalar."""
        return self._value_ns


class UpdateEvery(BaseModel):
    """Settings for the update frequency of the accumulation period."""

    value: float = Field(default=1.0, description="Update frequency.")
    unit: TimeUnit = Field(default="s", description="Physical unit for time values.")
    _value_ns: int | None = None

    def model_post_init(self, /, __context: Any) -> None:
        """Perform relatively expensive operations after model initialization."""
        self._value_ns = int(sc.scalar(self.value, unit=self.unit).to(unit='ns').value)

    @property
    def value_ns(self) -> int:
        """Update frequency in nanoseconds."""
        return self._value_ns
