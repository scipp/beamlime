# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""
Models for configuration values that can be used to control Beamlime services via Kafka.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

import scipp as sc
from pydantic import BaseModel, Field, model_validator

TimeUnit = Literal['ns', 'us', 'ms', 's']


class TOAEdges(BaseModel):
    """Time of arrival edges filter settings."""

    low: float = Field(default=0.0, description="Lower bound of the time window.")
    high: float = Field(default=72_000.0, description="Upper bound of the time window.")
    num_edges: int = Field(
        default=100, ge=1, description="Number of edges for the histogram."
    )
    unit: TimeUnit = Field(default="us", description="Physical unit for time values.")

    @property
    def edges_ns(self) -> tuple[sc.Variable, sc.Variable]:
        """Time window edges in nanoseconds as scipp scalars."""
        return (
            sc.scalar(self.low, unit=self.unit).to(unit='ns'),
            sc.scalar(self.high, unit=self.unit).to(unit='ns'),
        )


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
    """Setting for pixel weighting."""

    enabled: bool = Field(default=False, description="Enable pixel weighting.")
    method: WeightingMethod = Field(
        default=WeightingMethod.PIXEL_NUMBER, description="Method for pixel weighting."
    )


class TimeModel(BaseModel):
    """Base model for time values with unit."""

    value: float = Field(default=0, description="Time value.")
    unit: TimeUnit = Field(
        default="ns", description="Physical unit for the time value."
    )

    _value_ns: int | None = None

    def model_post_init(self, /, __context: Any) -> None:
        """Perform relatively expensive operations after model initialization."""
        self._value_ns = int(
            sc.scalar(self.value, unit=self.unit).to(unit='ns', dtype='int64').value
        )

    @property
    def value_ns(self) -> int:
        """Time in nanoseconds."""
        return self._value_ns


class StartTime(TimeModel):
    """Setting for the start time of the accumulation period."""


class UpdateEvery(TimeModel):
    """Setting for the update frequency of the accumulation period."""

    value: float = Field(default=1.0, ge=0.1, description="Time value.")
    unit: TimeUnit = Field(default="s", description="Physical unit for the time value.")


class ROIAxisRange(BaseModel):
    """
    Setting for the range of an axis to use for a rectangular ROI.

    Low and high are given as fractions of the axis length between 0 and 1.
    """

    low: float = Field(
        ge=0.0, lt=1.0, default=0.49, description="Start of the ROI as a fraction."
    )
    high: float = Field(
        ge=0.0, lt=1.0, default=0.51, description="End of the ROI as a fraction."
    )

    @model_validator(mode='after')
    def validate_range(self) -> ROIAxisRange:
        """Validate that low < high."""
        if self.low >= self.high:
            raise ValueError('Low value must be less than high value')
        return self


class ROIRectangle(BaseModel):
    x: ROIAxisRange = Field(default_factory=ROIAxisRange)
    y: ROIAxisRange = Field(default_factory=ROIAxisRange)


class ConfigKey(BaseModel, frozen=True):
    """
    Model for configuration key structure.

    Configuration keys follow the format 'source_name/service_name/key', where:
    - source_name can be a specific source name or '*' for all sources
    - service_name can be a specific service name or '*' for all services
    - key is the specific configuration parameter name
    """

    source_name: str | None = Field(
        default=None,
        description="Source name, or None for wildcard (*) matching all sources",
    )
    service_name: str | None = Field(
        default=None,
        description="Service name, or None for wildcard (*) matching all services",
    )
    key: str = Field(description="Configuration parameter name/key")

    def __str__(self) -> str:
        """
        Convert the configuration key to its string representation.

        Returns
        -------
        :
            String in the format source_name/service_name/key with '*' for None values
        """
        source = '*' if self.source_name is None else self.source_name
        service = '*' if self.service_name is None else self.service_name
        return f"{source}/{service}/{self.key}"

    @classmethod
    def from_string(cls, key_str: str) -> ConfigKey:
        """
        Create a ConfigKey from its string representation.

        Parameters
        ----------
        key_str:
            String in the format 'source_name/service_name/key'

        Returns
        -------
        :
            A ConfigKey instance parsed from the string

        Raises
        ------
        ValueError:
            If the key format is invalid
        """
        parts = key_str.split('/')
        if len(parts) != 3:
            raise ValueError(
                "Invalid key format, expected 'source_name/service_name/key', "
                f"got {key_str}"
            )
        source_name, service_name, key = parts
        if source_name == '*':
            source_name = None
        if service_name == '*':
            service_name = None
        return cls(source_name=source_name, service_name=service_name, key=key)
