# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging

import scipp as sc

from beamlime.handlers.to_nx_log import ToNXlog

from ..core.handler import (
    Accumulator,
    Config,
    Handler,
    HandlerFactory,
    PeriodicAccumulatingHandler,
)
from ..core.message import MessageKey
from .accumulators import LogData


class Timeseries(Accumulator[sc.DataArray, sc.DataArray]):
    """
    Accumulate by appending to a timeseries.

    The input is a DataArray with a time dimension. The output is a DataArray with the
    same time dimension. The data variable may have additional dimensions, with time
    being the first dimension.
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
        while self._at_capacity(data.sizes['time']):
            self._timeseries = sc.concat(
                [self._timeseries, self._timeseries], dim='time'
            )
        sel = slice(self._end, self._end + data.sizes['time'])
        self._timeseries.coords['time'][sel] = data.coords['time']
        self._timeseries.data['time', sel] = data.data
        self._end += data.sizes['time']

    def get(self) -> sc.DataArray:
        if self._timeseries is None:
            raise ValueError("No data has been added")
        return self._timeseries['time', : self._end]

    def clear(self) -> None:
        self._end = 0


class LogdataHandlerFactory(HandlerFactory[LogData, sc.DataArray]):
    """
    Factory for creating handlers for log data.

    This factory creates a handler that accumulates log data and returns it as a
    DataArray.
    """

    def __init__(
        self,
        *,
        instrument: str,
        logger: logging.Logger | None = None,
        config: Config,
        attribute_registry: dict[str, dict[str, any]],
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._instrument = instrument
        self._attribute_registry = attribute_registry

    def make_handler(self, key: MessageKey) -> Handler[LogData, sc.DataArray] | None:
        source_name = key.source_name
        attrs = self._attribute_registry.get(source_name)
        if attrs is None:
            self._logger.warning(
                "No attributes found for source name %s. Messages will be dropped.",
                source_name,
            )
            return None
        preprocessor = ToNXlog(attrs=attrs)
        accumulators = {'timeseries': Timeseries()}
        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=self._config,
            preprocessor=preprocessor,
            accumulators=accumulators,
        )
