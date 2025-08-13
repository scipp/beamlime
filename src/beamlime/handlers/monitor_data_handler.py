# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Hashable

import pydantic
import scipp as sc

from .. import parameter_models
from ..config.instrument import Instrument
from ..core.handler import JobBasedHandlerFactoryBase
from ..core.job import StreamProcessor
from ..core.message import StreamId, StreamKind
from .accumulators import Accumulator, Cumulative, MonitorEvents
from .to_nxevent_data import ToNXevent_data

instrument = Instrument(name='beam_monitors')


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
def _monitor_data_workflow(params: MonitorDataParams) -> StreamProcessor:
    return MonitorStreamProcessor(edges=params.toa_edges.get_edges())


class MonitorHandlerFactory(
    JobBasedHandlerFactoryBase[MonitorEvents | sc.DataArray, sc.DataArray]
):
    def make_preprocessor(self, key: StreamId) -> Accumulator | None:
        match key.kind:
            case StreamKind.MONITOR_COUNTS:
                return Cumulative(clear_on_get=True)
            case StreamKind.MONITOR_EVENTS:
                return ToNXevent_data()
            case _:
                return None
