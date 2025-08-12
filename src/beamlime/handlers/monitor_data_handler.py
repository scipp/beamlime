# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from collections.abc import Hashable

import pydantic
import scipp as sc

from .. import parameter_models
from ..config.instruments.dream import instrument
from ..core.handler import (
    Config,
    ConfigRegistry,
    FakeConfigRegistry,
    HandlerFactory,
    PeriodicAccumulatingHandler,
)
from ..core.job import StreamProcessor
from ..core.message import StreamId, StreamKind
from .accumulators import (
    Accumulator,
    Cumulative,
    MonitorEvents,
    TOAHistogrammer,
    TOARebinner,
)


class MonitorDataParams(pydantic.BaseModel):
    toa_edges: parameter_models.TOAEdges = pydantic.Field(
        title="Time of Arrival Edges",
        description="Time of arrival edges for histogramming.",
        default=parameter_models.TOAEdges(
            start=0.0,
            stop=1000.0 / 14,
            num_bins=100,
            unit=parameter_models.TimeUnit.MS,
        ),
    )


class MonitorStreamProcessor(StreamProcessor):
    def __init__(self, edges: sc.Variable) -> None:
        self._edges = edges
        self._cumulative = Cumulative()
        self._current = Cumulative(clear_on_get=True)

    def accumulate(self, data: dict[Hashable, sc.DataArray]) -> None:
        # TODO Histogram if event data!
        if len(data) != 1:
            raise ValueError("MonitorStreamProcessor expects exactly one data item.")
        monitor = next(iter(data.values()))
        timestamp = 0
        self._cumulative.add(timestamp=timestamp, data=monitor)
        self._current.add(timestamp=timestamp, data=monitor)

    def _compute(self, accum: Cumulative) -> sc.DataArray:
        # TODO Copied from TOARebinner, should be refactored to not use config mechanism
        cumulative = accum.get()
        if cumulative.dim != 'time_of_arrival':
            cumulative = cumulative.rename({cumulative.dim: 'time_of_arrival'})
        coord = cumulative.coords.get(self._edges.dim).to(unit=self._edges.unit)
        return cumulative.assign_coords({self._edges.dim: coord}).rebin(
            {self._edges.dim: self._edges}
        )

    def finalize(self) -> dict[Hashable, sc.DataArray]:
        return {
            'cumulative': self._compute(self._cumulative),
            'current': self._compute(self._current),
        }

    def clear(self) -> None:
        self._cumulative.clear()
        self._current.clear()


@instrument.register_workflow(
    name='monitor_data',
    version=1,
    title="Beam monitor data",
    description="Histogrammed and time-integrated beam monitor data.",
    source_names=['monitor1', 'monitor2'],
)
def _monitor_data_workflow(
    source_name: str, params: MonitorDataParams
) -> StreamProcessor:
    return MonitorStreamProcessor(edges=params.toa_edges.get_edges())


def make_monitor_data_preprocessor(
    key: StreamId, config: Config
) -> Accumulator[MonitorEvents, sc.DataArray] | Accumulator[sc.DataArray, sc.DataArray]:
    match key.kind:
        case StreamKind.MONITOR_EVENTS:
            return TOAHistogrammer(config=config)
        case StreamKind.MONITOR_COUNTS:
            return TOARebinner(config=config, clear_on_get=True)
        case _:
            raise ValueError(f"Invalid stream kind: {key.kind}")


class MonitorHandlerFactory(HandlerFactory[MonitorEvents | sc.DataArray, sc.DataArray]):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        config_registry: ConfigRegistry | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config_registry = config_registry or FakeConfigRegistry()

    def make_handler(
        self, key: StreamId
    ) -> (
        PeriodicAccumulatingHandler[MonitorEvents, sc.DataArray]
        | PeriodicAccumulatingHandler[sc.DataArray, sc.DataArray]
    ):
        config = self._config_registry.get_config(key.name)
        accumulators = {
            'cumulative': Cumulative(config=config),
            'current': Cumulative(config=config, clear_on_get=True),
        }
        preprocessor = make_monitor_data_preprocessor(key, config)
        return PeriodicAccumulatingHandler(
            service_name=self._config_registry.service_name,
            logger=self._logger,
            config=config,
            preprocessor=preprocessor,
            accumulators=accumulators,
        )
