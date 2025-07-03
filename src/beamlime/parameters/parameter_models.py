# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""
Models for data reduction workflow inputs.

These models are used to define inputs to Sciline workflows. The allow for auto-creating
user interfaces for configuring workflows as well as validation of the inputs on the
frontend.
"""

from pydantic import BaseModel, Field, field_validator

from .parameter_registry import ParameterRegistry

_registry = ParameterRegistry()


@_registry.register(name='wavelength_edges', version=1)
class WavelengthEdges(BaseModel):
    """Model for wavelength edges."""

    start: float = Field(default=1.0, description="Start of the edges.")
    stop: float = Field(default=10.0, description="Stop of the edges.")
    num_bins: int = Field(default=100, ge=1, le=10000, description="Number of bins.")
    unit: str = Field(default='Å', description="Unit of the edges.")
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
