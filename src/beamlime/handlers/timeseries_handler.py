# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import TypeVar

import scipp as sc

from ..core.handler import Accumulator, Config, PeriodicAccumulatingHandler
from .accumulators import LogData


class LogDataPreprocessor(Accumulator[LogData, sc.DataArray]):
    """
    Preprocessor for log data.

    Concatenates LogData objects and returns a single DataArray.
    """

    def __init__(self) -> None:
        self._values: list[LogData] = []

    @property
    def _unit(self) -> str:
        if not self._values:
            raise ValueError("No data has been added")
        return self._values[0].unit

    def add(self, timestamp: int, data: LogData) -> None:
        if self._values:
            if self._unit != data.unit:
                raise ValueError("Cannot mix different units")
        self._values.append(data)

    def get(self) -> sc.DataArray:
        if not self._values:
            return sc.DataArray(
                sc.array(dims=['time'], values=[], unit=''),
                coords={'time': sc.array(dims=['time'], values=[], unit='ns')},
            )
        values = [data.value for data in self._values]
        times = [data.time for data in self._values]
        unit = self._unit
        self.clear()
        return sc.DataArray(
            sc.array(dims=['time'], values=values, unit=unit),
            coords={
                'time': sc.array(dims=['time'], values=times, unit='ns'),
            },
        )

    def clear(self) -> None:
        self._values.clear()


class Timeseries(Accumulator[sc.DataArray, sc.DataArray]):
    """
    Accumulate by appending to a timeseries.
    """

    def __init__(self) -> None:
        self._timeseries: sc.DataArray | None = None
        self._end = 0

    def _at_capacity(self, extra_size: int) -> bool:
        return self._end + extra_size > self._timeseries.sizes['time']

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        # Avoid overall quadratic cost by "doubling" the size of the array if it is at
        # capacity, similar to std::vector in C++.
        if self._timeseries is None:
            self._timeseries = sc.concat([data] * 2, dim='time')
        elif self._at_capacity(data.sizes['time']):
            self._timeseries = sc.concat([self._timeseries, data] * 2, dim='time')
        else:
            sel = slice(self._end, self._end + data.sizes['time'])
            self._timeseries.coords['time'][sel] = data.coords['time']
            self._timeseries.data['time', sel] = data.data
        self._end += data.sizes['time']

    def get(self) -> sc.DataArray:
        if self._timeseries is None:
            raise ValueError("No data has been added")
        return self._timeseries

    def clear(self) -> None:
        self._end = 0


T = TypeVar('T')


def create_logdata_handler(
    *, logger: logging.Logger | None = None, config: Config
) -> PeriodicAccumulatingHandler[T, sc.DataArray]:
    """Create a handler for monitor data."""
    accumulators = {'timeseries': Timeseries()}
    preprocessor = LogDataPreprocessor()
    return PeriodicAccumulatingHandler(
        logger=logger,
        config=config,
        preprocessor=preprocessor,
        accumulators=accumulators,
    )
