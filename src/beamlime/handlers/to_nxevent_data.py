# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TypeVar

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

from beamlime.core.handler import Accumulator


def _require_single_pulse(ev44: eventdata_ev44.EventData) -> None:
    index = np.asarray(ev44.reference_time_index)
    if index.shape != (1,) or index != 0 or len(ev44.reference_time) > 1:
        raise NotImplementedError("Processing multi-pulse messages is not supported.")


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
        _require_single_pulse(ev44)
        return MonitorEvents(time_of_arrival=ev44.time_of_flight, unit='ns')


@dataclass
class DetectorEvents(MonitorEvents):
    """
    Dataclass for detector events.

    Decouples our handlers from upstream schema changes. This also simplifies handler
    testing since tests do not have to construct a full eventdata_ev44.EventData object.

    Note that we keep the raw array of time of arrivals, and the unit. This is to avoid
    unnecessary copies of the data.
    """

    pixel_id: Sequence[int]

    def __post_init__(self) -> None:
        if len(self.pixel_id) != len(self.time_of_arrival):
            raise ValueError(
                f"pixel_id and time_of_arrival must have the same length, "
                f"got {len(self.pixel_id)} and {len(self.time_of_arrival)}"
            )

    @staticmethod
    def from_ev44(ev44: eventdata_ev44.EventData) -> DetectorEvents:
        _require_single_pulse(ev44)
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
        self._timestamps.append(int(timestamp))
        self._chunks.append(data)

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
