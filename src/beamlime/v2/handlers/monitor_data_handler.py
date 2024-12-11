# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging
from dataclasses import replace

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

from beamlime.v2.core.handler import Config, Handler, Message


class MonitorDataHandler(Handler[eventdata_ev44.EventData, sc.DataArray]):
    def __init__(self, *, logger: logging.Logger | None = None, config: Config):
        super().__init__(logger=logger, config=config)
        # TODO do this in handle() again!
        self._update_every = self._config.get("update_every_seconds", 1.0) * 1e9  # ns
        # window_length = self._config.get("window_length", 10.0)
        # self._update_every = sc.scalar(update_every, unit='s').to(
        #    unit='ns', dtype='int64'
        # )
        # self._window_length = round(window_length / update_every)
        self._next_update: int | None = None
        self._histogrammer = Histogrammer()
        self._edges = sc.linspace('time_of_arrival', 0.0, 1000 / 14, num=5, unit='ms')
        self._cumulative = Cumulative()
        window_length = 10.0
        self._sliding_window = SlidingWindow(sc.scalar(window_length, unit='s'))

    def handle(
        self, message: Message[eventdata_ev44.EventData]
    ) -> list[Message[sc.DataArray]]:
        # handle "start" (clear history) <= something should translate run_start message
        #  to this as well
        # publish sum since start as well as sliding window... but only less frequently
        # config: window length, bin count
        # do not include edges every time to save space. Or send only params?
        # changing bin count cannot not affect the sum since start! Need to hist twice?
        # Should include info such as time interval of the published result
        # unit should be per counts/second or maybe even counts/second/ms

        # add abstraction layer so we can later handle da00 as well, i.e., to histogram
        # in first step, all window handling or summation based on histogrammed data

        self._histogrammer.add(message.value.time_of_flight)
        reference_time = message.timestamp
        if self._next_update is None:
            self._next_update = reference_time
        key = message.key
        if reference_time < self._next_update:
            return []
        hist = self._histogrammer.histogram(self._edges)
        self._cumulative.add(hist)
        self._sliding_window.add(timestamp=reference_time, data=hist)
        # If there were no pulses for a while we need to skip several updates.
        # Note that we do not simply set _next_update based on reference_time
        # to avoid drifts.
        self._next_update += (
            (reference_time - self._next_update) // self._update_every + 1
        ) * self._update_every
        return [
            Message(
                timestamp=reference_time,
                key=replace(key, topic=f'{key.topic}.sliding'),
                value=self._sliding_window.get(),
            ),
            Message(
                timestamp=reference_time,
                key=replace(key, topic=f'{key.topic}.cumulative'),
                value=self._cumulative.get(),
            ),
        ]


class Cumulative:
    def __init__(self):
        self._cumulative: sc.DataArray | None = None

    def add(self, data: sc.DataArray) -> None:
        if self._cumulative is None:
            self._cumulative = data.copy()
        else:
            self._cumulative += data

    def get(self) -> sc.DataArray:
        return self._cumulative


class SlidingWindow:
    def __init__(self, max_age: sc.Variable):
        self._max_age = max_age.to(unit='ns', dtype='int64')
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


class Histogrammer:
    def __init__(self):
        self._chunks: list[np.ndarray] = []

    def add(self, events: np.ndarray) -> None:
        self._chunks.append(events)

    def histogram(self, bins: sc.Variable) -> sc.DataArray:
        # Using NumPy here as for these specific operations with medium-sized data it is
        # a bit faster than Scipp. Could optimize the concatenate by reusing a buffer.
        events = np.concatenate(self._chunks or [[]])
        values, _ = np.histogram(events, bins=bins.to(unit='ns').values)
        self._chunks.clear()
        return sc.DataArray(
            data=sc.array(dims=[bins.dim], values=values, unit='counts'),
            coords={bins.dim: bins},
        )
