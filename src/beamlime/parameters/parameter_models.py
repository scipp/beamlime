# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""
Models for data reduction workflow inputs.

These models are used to define inputs to Sciline workflows. The allow for auto-creating
user interfaces for configuring workflows as well as validation of the inputs on the
frontend.
"""

from abc import ABC
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from .parameter_registry import ParameterRegistry

_registry = ParameterRegistry()


class RangeModel(BaseModel, ABC):
    """Base model for range with common fields and validation."""

    start: float = Field(default=0.0, description="Start of the range.")
    stop: float = Field(default=10.0, description="Stop of the range.")

    @field_validator('stop')
    @classmethod
    def stop_must_be_greater_than_start(cls, v, info):
        start = info.data.get('start')
        if start is not None and v <= start:
            raise ValueError('stop must be greater than start')
        return v


class EdgesModel(BaseModel, ABC):
    """Base model for edges with common fields and validation."""

    start: float = Field(default=1.0, description="Start of the edges.")
    stop: float = Field(default=10.0, description="Stop of the edges.")
    num_bins: int = Field(default=100, ge=1, le=10000, description="Number of bins.")
    log: bool = Field(
        default=False, description="If True, use logarithmically spaced edges."
    )

    @field_validator('stop')
    @classmethod
    def stop_must_be_greater_than_start(cls, v, info):
        start = info.data.get('start')
        if start is not None and v <= start:
            raise ValueError('stop must be greater than start')
        return v

    @field_validator('start')
    @classmethod
    def start_must_be_positive_if_log(cls, v, info):
        log = info.data.get('log')
        if log and v <= 0:
            raise ValueError('start must be positive if log is True')
        return v


class WavelengthUnit(str, Enum):
    """Allowed units for wavelength."""

    ANGSTROM = 'Å'
    NANOMETER = 'nm'


class DspacingUnit(str, Enum):
    """Allowed units for d-spacing."""

    ANGSTROM = 'Å'
    NANOMETER = 'nm'


@_registry.register(name='filename', version=1)
class Filename(BaseModel):
    value: Path = Field(..., description="Path to the file.")


@_registry.register(name='wavelength_range', version=1)
class WavelengthRange(RangeModel):
    """Model for wavelength range."""

    unit: WavelengthUnit = Field(
        default=WavelengthUnit.ANGSTROM, description="Unit of the wavelength range."
    )


@_registry.register(name='wavelength_edges', version=1)
class WavelengthEdges(EdgesModel):
    """Model for wavelength edges."""

    unit: WavelengthUnit = Field(
        default=WavelengthUnit.ANGSTROM, description="Unit of the edges."
    )


@_registry.register(name='dspacing_edges', version=1)
class DspacingEdges(EdgesModel):
    """Model for d-spacing edges."""

    unit: DspacingUnit = Field(
        default=DspacingUnit.ANGSTROM, description="Unit of the edges."
    )
