# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Generic, Protocol, TypeVar

import numpy as np
import scipp as sc
from streaming_data_types import eventdata_ev44

from ..core.handler import Config, Handler, Message


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


T = TypeVar('T')
U = TypeVar('U')


class Preprocessor(Protocol, Generic[T, U]):
    def add(self, data: T) -> None:
        pass

    def get(self) -> U:
        pass


class Accumulator(Protocol, Generic[T]):
    def add(self, timestamp: int, data: T) -> None:
        pass

    def get(self) -> T:
        pass


class GenericHandler(Handler[T, sc.DataArray]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config: Config,
        preprocessor: Preprocessor[T, sc.DataArray],
        accumulators: dict[str, Accumulator[sc.DataArray]],
    ):
        super().__init__(logger=logger, config=config)
        self._preprocessor = preprocessor
        self._accumulators = accumulators
        self._update_every = int(
            self._config.get("update_every_seconds", 1.0) * 1e9
        )  # ns
        self._next_update: int = 0

    def handle(self, message: Message[T]) -> list[Message[sc.DataArray]]:
        # TODO Config updates!
        self._preprocessor.add(message.value)
        if message.timestamp < self._next_update:
            return []
        data = self._preprocessor.get()
        for accumulator in self._accumulators.values():
            accumulator.add(timestamp=message.timestamp, data=data)
        # If there were no pulses for a while we need to skip several updates.
        # Note that we do not simply set _next_update based on reference_time
        # to avoid drifts.
        self._next_update += (
            (message.timestamp - self._next_update) // self._update_every + 1
        ) * self._update_every
        key = message.key
        return [
            Message(
                timestamp=message.timestamp,
                key=replace(key, topic=f'{key.topic}_{name}'),
                value=accumulator.get(),
            )
            for name, accumulator in self._accumulators.items()
        ]


class MonitorDataHandler(GenericHandler[MonitorEvents]):
    def __init__(self, *, logger: logging.Logger | None = None, config: Config):
        preprocessor = Histogrammer(config=config)
        accumulators = {
            'cumulative': Cumulative(config=config),
            'sliding': SlidingWindow(config=config),
        }
        super().__init__(
            logger=logger,
            config=config,
            preprocessor=preprocessor,
            accumulators=accumulators,
        )


class Cumulative(Accumulator[sc.DataArray]):
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


class SlidingWindow(Accumulator[sc.DataArray]):
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


class Histogrammer(Preprocessor[MonitorEvents, sc.DataArray]):
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

    def add(self, data: MonitorEvents) -> None:
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
