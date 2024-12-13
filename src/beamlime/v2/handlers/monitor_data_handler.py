# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TypeVar

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

from ..core.handler import (
    Accumulator,
    Config,
    PeriodicAccumulatingHandler,
)


@dataclass
class MonitorEvents:
    """
    Dataclass for monitor events.

    Decouples our handlers from upstream schema changes. This also simplifies handler
    testing since tests do not have to construct a full eventdata_ev44.EventData object.
    """

    time_of_arrival: Sequence[int]

    @staticmethod
    def from_ev44(ev44: eventdata_ev44.EventData) -> MonitorEvents:
        return MonitorEvents(time_of_arrival=ev44.time_of_flight)


def create_monitor_event_data_handler(
    *, logger: logging.Logger | None = None, config: Config
) -> PeriodicAccumulatingHandler[MonitorEvents, sc.DataArray]:
    return _create_monitor_handler(
        logger=logger, config=config, preprocessor=Histogrammer(config=config)
    )


def create_monitor_data_handler(
    *, logger: logging.Logger | None = None, config: Config
) -> PeriodicAccumulatingHandler[sc.DataArray, sc.DataArray]:
    return _create_monitor_handler(
        logger=logger, config=config, preprocessor=Cumulative(config=config)
    )


T = TypeVar('T')


def _create_monitor_handler(
    *,
    logger: logging.Logger | None = None,
    config: Config,
    preprocessor: Accumulator[T, sc.DataArray],
) -> PeriodicAccumulatingHandler[T, sc.DataArray]:
    accumulators = {
        'cumulative': Cumulative(config=config),
        'sliding': SlidingWindow(config=config),
    }
    return PeriodicAccumulatingHandler(
        logger=logger,
        config=config,
        preprocessor=preprocessor,
        accumulators=accumulators,
    )


class Cumulative(Accumulator[sc.DataArray, sc.DataArray]):
    def __init__(self, config: Config):
        self._config = config
        # TODO We will want to support clearing the history, e.g., when a "start"
        # message is received. This is not yet implemented. This could be automatic,
        # based on run_start messages from upstream, or via our own control topic. We
        # can consider translating run_start messages to our own control messages.
        self._cumulative: sc.DataArray | None = None

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        _ = timestamp
        if self._cumulative is None:
            self._cumulative = data.copy()
        else:
            self._cumulative += data

    def get(self) -> sc.DataArray:
        return self._cumulative


class SlidingWindow(Accumulator[sc.DataArray, sc.DataArray]):
    def __init__(self, config: Config):
        self._config = config
        self._max_age = sc.scalar(
            self._config.get('sliding_window_seconds', 3.0), unit='s'
        ).to(unit='ns', dtype='int64')
        self._chunks: list[sc.DataArray] = []

    def add(self, *, timestamp: int, data: sc.DataArray) -> None:
        self._chunks.append(
            data.assign_coords({'time': sc.scalar(timestamp, unit='ns')})
        )

    def get(self) -> sc.DataArray:
        self._cleanup()
        # sc.reduce returns inconsistent result with/without `time` coord depending on
        # the number of chunks. We remove it to ensure consistent behavior.
        result = sc.reduce(self._chunks).sum()
        result.coords.pop('time', None)
        return result

    def _cleanup(self) -> None:
        latest = sc.reduce([chunk.coords['time'] for chunk in self._chunks]).max()
        self._chunks = [
            chunk
            for chunk in self._chunks
            if latest - chunk.coords['time'] <= self._max_age
        ]


class Histogrammer(Accumulator[MonitorEvents, sc.DataArray]):
    """
    Accumulator that bins time of arrival data into a histogram.

    Monitor data handlers use this as a preprocessor before actual accumulation.
    """

    def __init__(self, config: Config):
        self._config = config
        self._nbin = -1
        self._chunks: list[np.ndarray] = []
        self._edges: sc.Variable | None = None
        self._edges_ns: sc.Variable | None = None

    def _check_for_config_updates(self) -> None:
        nbin = self._config.get('time_of_arrival_bins', 100)
        if self._edges is None or nbin != self._nbin:
            self._nbin = nbin
            self._edges = sc.linspace(
                'time_of_arrival', 0.0, 1000 / 14, num=nbin, unit='ms'
            )
            self._edges_ns = self._edges.to(unit='ns')

    def add(self, timestamp: int, data: MonitorEvents) -> None:
        _ = timestamp
        self._chunks.append(data.time_of_arrival)

    def get(self) -> sc.DataArray:
        # TODO Changing bin count needs to be handled in accumulators, currently is not.
        self._check_for_config_updates()
        # Using NumPy here as for these specific operations with medium-sized data it is
        # a bit faster than Scipp. Could optimize the concatenate by reusing a buffer.
        events = np.concatenate(self._chunks or [[]])
        values, _ = np.histogram(events, bins=self._edges_ns.values)
        self._chunks.clear()
        return sc.DataArray(
            data=sc.array(dims=[self._edges.dim], values=values, unit='counts'),
            coords={self._edges.dim: self._edges},
        )


class Rebinner:
    def __init__(self):
        self._arrays: list[sc.DataArray] = []

    def histogram(self, edges: sc.Variable) -> sc.DataArray:
        da = sc.reduce(self._arrays).sum()
        self._arrays.clear()
        return da.rebin({edges.dim: edges})
