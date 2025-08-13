# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from collections.abc import Hashable

import pydantic
import scipp as sc

from .. import parameter_models
from ..config.instrument import Instrument, StreamProcessor
from ..core.handler import JobBasedHandlerFactoryBase
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
        self._event_edges = edges.to(unit='ns').rename({edges.dim: 'event_time_offset'})
        self._cumulative: sc.DataArray | None = None
        self._current: sc.DataArray | None = None

    def accumulate(self, data: dict[Hashable, sc.DataArray]) -> None:
        if len(data) != 1:
            raise ValueError("MonitorStreamProcessor expects exactly one data item.")
        da = next(iter(data.values()))
        # Note: In theory we should consider rebinning/histogramming only in finalize(),
        # but the current plan is to accumulate before/during preprocessing, i.e.,
        # before we ever get here. That is, there should typically be one finalize()
        # call per accumulate() call.
        if da.bins is not None:
            da = (
                da.hist(event_time_offset=self._event_edges, dim=da.dims)
                .drop_coords('event_time_offset')
                .rename_dims(event_time_offset=self._edges.dim)
                .assign_coords({self._edges.dim: self._edges})
            )
        else:
            if da.dim != self._edges.dim:
                da = da.rename({da.dim: self._edges.dim})
            coord = da.coords.get(self._edges.dim).to(unit=self._edges.unit)
            da = da.assign_coords({self._edges.dim: coord}).rebin(
                {self._edges.dim: self._edges}
            )
        if self._current is None:
            self._current = da
        else:
            self._current += da

    def finalize(self) -> dict[Hashable, sc.DataArray]:
        if self._current is None:
            raise ValueError("No data has been added")
        current = self._current
        if self._cumulative is None:
            self._cumulative = current
        else:
            self._cumulative += current
        self._current = None
        return {'cumulative': self._cumulative, 'current': current}

    def clear(self) -> None:
        self._cumulative = None
        self._current = None


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
