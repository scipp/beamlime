# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

import numpy as np
import scipp as sc
from ess.reduce.live.roi import ROIFilter
from scipp._scipp.core import _bins_no_validate
from streaming_data_types import eventdata_ev44

from ..config import models
from ..core.handler import Accumulator, Config, ConfigModelAccessor


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


Events = TypeVar('Events', DetectorEvents, MonitorEvents)


class ToNXevent_data(Accumulator[Events, sc.DataArray]):
    def __init__(self):
        self._chunks: list[Events] = []
        self._timestamps: list[int] = []
        self._epoch = sc.epoch(unit='ns')
        self._have_event_id: bool | None = None
        self._toa_dtype = np.int64

    def add(self, timestamp: int, data: Events) -> None:
        if data.unit != 'ns':
            raise ValueError(f"Expected unit 'ns', got '{data.unit}'")
        if self._have_event_id is None:
            self._have_event_id = isinstance(data, DetectorEvents)
            self._toa_dtype = np.asarray(data.time_of_arrival).dtype
        elif self._have_event_id != isinstance(data, DetectorEvents):
            # This should never happen, but we check to be safe.
            raise ValueError("Inconsistent event_id")
        self._chunks.append(data)
        self._timestamps.append(timestamp)

    def get(self) -> sc.DataArray:
        if self._have_event_id is None:
            raise ValueError("No data has been added")
        if self._chunks:
            toa_values = np.concatenate([d.time_of_arrival for d in self._chunks])
        else:
            toa_values = np.array([], dtype=self._toa_dtype)
        event_time_offset = sc.array(dims=['event'], values=toa_values, unit='ns')
        weights = sc.ones(sizes=event_time_offset.sizes, dtype='float32', unit='counts')
        events = sc.DataArray(
            data=weights, coords={'event_time_offset': event_time_offset}
        )
        if self._have_event_id:
            if self._chunks:
                ids = np.concatenate([d.pixel_id for d in self._chunks])
            else:
                ids = np.array([])
            event_id = sc.array(dims=['event'], values=ids, unit=None, dtype='int32')
            events.coords['event_id'] = event_id

        lens = [len(d.time_of_arrival) for d in self._chunks]
        sizes = sc.array(
            dims=['event_time_zero'], values=lens, unit=None, dtype='int64'
        )
        begin = sc.cumsum(sizes, mode='exclusive')
        binned = sc.DataArray(sc.bins(begin=begin, dim='event', data=events))
        binned.coords['event_time_zero'] = self._epoch + sc.array(
            dims=['event_time_zero'], values=self._timestamps, unit='ns', dtype='int64'
        )
        self.clear()
        return binned

    def clear(self) -> None:
        self._chunks.clear()
        self._timestamps.clear()


class NullAccumulator(Accumulator[Any, None]):
    def add(self, timestamp: int, data: Any) -> None:
        pass

    def get(self) -> None:
        return None

    def clear(self) -> None:
        pass


class Cumulative(Accumulator[sc.DataArray, sc.DataArray]):
    def __init__(self, config: Config | None = None, clear_on_get: bool = False):
        self._config = config or {}
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


class GroupIntoPixels(Accumulator[DetectorEvents, sc.DataArray]):
    def __init__(self, config: Config, detector_number: sc.Variable):
        self._config = config
        self._chunks: list[DetectorEvents] = []
        self._toa_unit = 'ns'
        self._sizes = detector_number.sizes
        self._dim = 'detector_number'
        self._groups = detector_number.flatten(to=self._dim)

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
            coords={self._dim: sc.array(dims=['event'], values=pixel_ids, unit=None)},
        )
        self._chunks.clear()
        return da.group(self._groups).fold(dim=self._dim, sizes=self._sizes)

    def clear(self) -> None:
        self._chunks.clear()


class ROIBasedTOAHistogram(Accumulator[sc.DataArray, sc.DataArray]):
    def __init__(self, config: Config, roi_filter: ROIFilter):
        self._config = config
        self._roi_filter = roi_filter
        self._chunks: list[sc.DataArray] = []

        # Note: Currently we are using the same ROI config values for all detector
        # handlers ands views. This is for demo purposes and will be replaced by a more
        # flexible configuration in the future.
        self._roi = ConfigModelAccessor(
            config=config, key='roi_rectangle', model=models.ROIRectangle
        )
        self._nbin = -1
        self._edges: sc.Variable | None = None
        self._edges_ns: sc.Variable | None = None
        self._current_roi = None

    def _check_for_config_updates(self) -> None:
        nbin = self._config.get('time_of_arrival_bins', 100)
        if self._edges is None or nbin != self._nbin:
            self._nbin = nbin
            self._edges = sc.linspace(
                'time_of_arrival', 0.0, 1000 / 14, num=nbin, unit='ms'
            )
            self._edges_ns = self._edges.to(unit='ns')
            self.clear()

        # Access to protected variables should hopefully be avoided by changing the
        # config values to send indices instead of percentages, once we have per-view
        # configuration.
        y, x = self._roi_filter._indices.dims
        sizes = self._roi_filter._indices.sizes
        roi = self._roi()
        ry = roi.x
        rx = roi.y
        # Convert fraction to indices
        y_indices = (int(ry.low * (sizes[y] - 1)), int(ry.high * (sizes[y] - 1)))
        x_indices = (int(rx.low * (sizes[x] - 1)), int(rx.high * (sizes[x] - 1)))
        new_roi = {y: y_indices, x: x_indices}

        if new_roi != self._current_roi:
            self._current_roi = new_roi
            self._roi_filter.set_roi_from_intervals(sc.DataGroup(new_roi))
            self.clear()  # Clear accumulated data when ROI changes

    def _add_weights(self, data: sc.DataArray) -> None:
        constituents = data.bins.constituents
        content = constituents['data']
        content.coords['time_of_arrival'] = content.data
        content.data = sc.ones(
            dims=content.dims, shape=content.shape, dtype='float32', unit='counts'
        )
        data.data = _bins_no_validate(**constituents)

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        self._check_for_config_updates()
        # Note that the preprocessor does *not* add weights of 1 (unlike NeXus loaders).
        # Instead, the data column of the content corresponds to the time of arrival.
        filtered, scale = self._roi_filter.apply(data)
        self._add_weights(filtered)
        filtered *= scale
        chunk = filtered.hist(time_of_arrival=self._edges_ns, dim=filtered.dim)
        self._chunks.append(chunk)

    def get(self) -> sc.DataArray:
        da = sc.reduce(self._chunks).sum()
        self._chunks.clear()
        da.coords['time_of_arrival'] = self._edges
        return da

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
