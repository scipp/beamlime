# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

from ..core.handler import Accumulator, Config


@dataclass
class MonitorEvents:
    """
    Dataclass for monitor events.

    Decouples our handlers from upstream schema changes. This also simplifies handler
    testing since tests do not have to construct a full eventdata_ev44.EventData object.

    Note that we keep the raw array of time of arrivals, and the unit. This is to avoid
    unnecessary copies of the data.
    """

    time_of_arrival: Sequence[int]
    unit: str

    @staticmethod
    def from_ev44(ev44: eventdata_ev44.EventData) -> MonitorEvents:
        return MonitorEvents(time_of_arrival=ev44.time_of_flight, unit='ns')


class Cumulative(Accumulator[sc.DataArray, sc.DataArray]):
    def __init__(self, config: Config, clear_on_get: bool = False):
        self._config = config
        self._clear_on_get = clear_on_get
        self._cumulative: sc.DataArray | None = None

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        _ = timestamp
        if self._cumulative is None or data.sizes != self._cumulative.sizes:
            self._cumulative = data.copy()
        else:
            self._cumulative += data

    def get(self) -> sc.DataArray:
        if self._cumulative is None:
            raise ValueError("No data has been added")
        value = self._cumulative
        if self._clear_on_get:
            self._cumulative = None
        return value

    def clear(self) -> None:
        self._cumulative = None


class SlidingWindow(Accumulator[sc.DataArray, sc.DataArray]):
    def __init__(self, config: Config):
        self._config = config
        self._max_age = sc.scalar(
            self._config.get('sliding_window_seconds', 1.0), unit='s'
        ).to(unit='ns', dtype='int64')
        self._chunks: list[sc.DataArray] = []

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        if self._chunks and data.sizes != self._chunks[0].sizes:
            self.clear()
        self._chunks.append(
            data.assign_coords({'time': sc.scalar(timestamp, unit='ns')})
        )

    def get(self) -> sc.DataArray:
        if not self._chunks:
            raise ValueError("No data has been added")
        self._cleanup()
        # sc.reduce returns inconsistent result with/without `time` coord depending on
        # the number of chunks. We remove it to ensure consistent behavior.
        result = sc.reduce(self._chunks).sum()
        result.coords.pop('time', None)
        return result

    def clear(self) -> None:
        self._chunks.clear()

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
        # We could easily support other units, but ev44 is always in ns so this should
        # never happen.
        if data.unit != 'ns':
            raise ValueError(f"Expected unit 'ns', got '{data.unit}'")
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

    def clear(self) -> None:
        self._chunks.clear()


class Rebinner:
    def __init__(self):
        self._arrays: list[sc.DataArray] = []

    def histogram(self, edges: sc.Variable) -> sc.DataArray:
        da = sc.reduce(self._arrays).sum()
        self._arrays.clear()
        return da.rebin({edges.dim: edges})
