# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import TypeVar

import scipp as sc

from ..core.handler import Accumulator, Config, PeriodicAccumulatingHandler
from .accumulators import Cumulative, MonitorEvents, TOAHistogrammer


class MonitorDataPreprocessor(Accumulator[MonitorEvents | sc.DataArray, sc.DataArray]):
    """
    Preprocessor for monitor data.

    The kind of preprocessing (histogramming or cumulative sum) is determined by the
    type (events or histogram) of the first data added.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._accumulator: (
            Accumulator[MonitorEvents, sc.DataArray]
            | Accumulator[sc.DataArray, sc.DataArray]
            | None
        ) = None
        self._type: type[MonitorEvents | sc.DataArray] | None = None

    def _initialize(self, data: MonitorEvents | sc.DataArray) -> None:
        if self._type is None:
            self._type = type(data)
        elif type(data) is not self._type:
            raise ValueError("Cannot mix MonitorEvents and sc.DataArray")
        else:
            return
        if isinstance(data, MonitorEvents):
            self._accumulator = TOAHistogrammer(config=self._config)
        else:
            self._accumulator = Cumulative(config=self._config, clear_on_get=True)

    def add(self, timestamp: int, data: T) -> None:
        self._initialize(data)
        self._accumulator.add(timestamp, data)

    def get(self) -> sc.DataArray:
        if self._accumulator is None:
            raise ValueError("No data has been added")
        return self._accumulator.get()

    def clear(self) -> None:
        if self._accumulator is not None:
            self._accumulator.clear()


T = TypeVar('T')


def create_monitor_data_handler(
    *, logger: logging.Logger | None = None, config: Config
) -> PeriodicAccumulatingHandler[T, sc.DataArray]:
    """Create a handler for monitor data."""
    accumulators = {
        'cumulative': Cumulative(config=config),
        'current': Cumulative(config=config, clear_on_get=True),
    }
    preprocessor = MonitorDataPreprocessor(config=config)
    return PeriodicAccumulatingHandler(
        logger=logger,
        config=config,
        preprocessor=preprocessor,
        accumulators=accumulators,
    )
