# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

from ..core.handler import Accumulator, Config, ConfigValueAccessor


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


@dataclass
class DetectorEvents:
    """
    Dataclass for detector events.

    Decouples our handlers from upstream schema changes. This also simplifies handler
    testing since tests do not have to construct a full eventdata_ev44.EventData object.

    Note that we keep the raw array of time of arrivals, and the unit. This is to avoid
    unnecessary copies of the data.
    """

    pixel_id: Sequence[int]
    time_of_arrival: Sequence[int]
    unit: str

    @staticmethod
    def from_ev44(ev44: eventdata_ev44.EventData) -> DetectorEvents:
        return DetectorEvents(
            pixel_id=ev44.pixel_id, time_of_arrival=ev44.time_of_flight, unit='ns'
        )


class NullAccumulator(Accumulator[Any, None]):
    def add(self, timestamp: int, data: Any) -> None:
        pass

    def get(self) -> None:
        return None

    def clear(self) -> None:
        pass


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


@dataclass
class _Chunk:
    timestamp: int
    data: sc.DataArray


class SlidingWindow(Accumulator[sc.DataArray, sc.DataArray]):
    def __init__(self, config: Config):
        self._config = config
        self._chunks: list[_Chunk] = []
        self._max_age = ConfigValueAccessor(
            config=config,
            key='sliding_window',
            default={'value': 5.0, 'unit': 's'},
            convert=lambda raw: sc.scalar(**raw).to(unit='ns', dtype='int64').value,
        )

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        if self._chunks and data.sizes != self._chunks[0].data.sizes:
            self.clear()
        self._chunks.append(_Chunk(timestamp, data))

    def get(self) -> sc.DataArray:
        if not self._chunks:
            raise ValueError("No data has been added")
        self._cleanup()
        return sc.reduce([chunk.data for chunk in self._chunks]).sum()

    def clear(self) -> None:
        self._chunks.clear()

    def _cleanup(self) -> None:
        latest = max([chunk.timestamp for chunk in self._chunks])
        max_age = self._max_age()
        self._chunks = [c for c in self._chunks if latest - c.timestamp <= max_age]


# TODO
# - update RollingDetectorView
# - add ROI accumulator producing a TOA histogram
class GroupIntoPixels(Accumulator[DetectorEvents, sc.DataArray]):
    def __init__(self, config: Config, detector_number: sc.Variable):
        self._config = config
        self._chunks: list[DetectorEvents] = []
        self._toa_unit = 'ns'
        self._detector_number = detector_number.rename_dims(
            {detector_number.dim: 'detector_number'}
        )

    def add(self, timestamp: int, data: DetectorEvents) -> None:
        # timestamp in function signature is required for compliance with Accumulator
        # interface.
        _ = timestamp
        # We could easily support other units, but ev44 is always in ns so this should
        # never happen.
        if data.unit != self._toa_unit:
            raise ValueError(f"Expected unit '{self._toa_unit}', got '{data.unit}'")
        self._chunks.append(data)

    def get(self) -> sc.DataArray:
        # Could optimize the concatenate by reusing a buffer (directly write to it in
        # self.add).
        pixel_ids = np.concatenate([c.pixel_id for c in self._chunks])
        time_of_arrival = np.concatenate([c.time_of_arrival for c in self._chunks])
        da = sc.DataArray(
            data=sc.array(dims=['event'], values=time_of_arrival, unit=self._toa_unit),
            coords={
                'detector_number': sc.array(dims=['event'], values=pixel_ids, unit=None)
            },
        )
        self._chunks.clear()
        return da.group(self._detector_number)

    def clear(self) -> None:
        self._chunks.clear()


class TOAHistogrammer(Accumulator[DetectorEvents | MonitorEvents, sc.DataArray]):
    """
    Accumulator that bins time of arrival data into a histogram.

    Monitor data handlers use this as a preprocessor before actual accumulation. For
    detector data it could be used to produce a histogram for a selected ROI.
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

    def add(self, timestamp: int, data: DetectorEvents | MonitorEvents) -> None:
        _ = timestamp
        # We could easily support other units, but ev44 is always in ns so this should
        # never happen.
        if data.unit != 'ns':
            raise ValueError(f"Expected unit 'ns', got '{data.unit}'")
        self._chunks.append(data.time_of_arrival)

    def get(self) -> sc.DataArray:
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
