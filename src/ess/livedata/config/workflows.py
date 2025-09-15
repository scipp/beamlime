# SPDX-FileCopyrightText: 2025 Scipp contributors (https://github.com/scipp)
# SPDX-License-Identifier: BSD-3-Clause
"""Common workflows that are used by multiple instruments."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Hashable, NewType

import pydantic
import sciline
import scipp as sc

from ess.livedata import parameter_models
from ess.livedata.config import Instrument
from ess.reduce import streaming
from ess.reduce.nexus.types import Filename, MonitorData, NeXusData, NeXusName
from ess.reduce.time_of_flight import GenericTofWorkflow


class MonitorTimeseriesParams(pydantic.BaseModel):
    """Parameters for the monitor timeseries workflow."""

    toa_range: parameter_models.TOARange = pydantic.Field(
        title="Time of Arrival Range",
        description="Time of arrival range to include.",
        default=parameter_models.TOARange(),
    )


# TODO Instrument.source_to_key??
# TODO How to get current time?

CustomMonitor = NewType('CustomMonitor', int)
CurrentRun = NewType('CurrentRun', int)
MonitorCountsInInterval = NewType('MonitorCountsInInterval', sc.DataArray)


def _get_interval(
    data: MonitorData[CurrentRun, CustomMonitor], range: parameter_models.TOARange
) -> MonitorCountsInInterval:
    start, stop = range.range_ns
    if data.bins is not None:
        counts = data.bins['event_time_offset', start:stop].sum()
        counts.coords['time'] = data.coords['event_time_zero'][0]
    else:
        counts = data['event_time_offset', start:stop].sum()
        # TODO start_time from WorkflowData in Job?
        # counts.coords['time'] = data.coords['start_time']
    print(counts)
    return MonitorCountsInInterval(counts)


class TimeseriesAccumulator(streaming.Accumulator[sc.DataArray]):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._buffer: sc.DataGroup | None = None
        self._size: int = 0

    @property
    def is_empty(self) -> bool:
        return self._buffer is None

    def _get_value(self) -> sc.DataArray:
        if self._buffer is None:
            raise ValueError("No data accumulated")
        return sc.DataArray(
            data=self._buffer['data']['time', : self._size].copy(),
            coords={'time': self._buffer['time']['time', : self._size].copy()},
        )

    def _unpack(self, value: sc.DataArray) -> sc.DataGroup:
        return sc.DataGroup(data=value.data, time=value.coords['time'])

    def _do_push(self, value: sc.DataArray) -> None:
        if self._buffer is None:
            # Initialize buffer with the first value and ensure we have 'time' dim
            self._buffer = sc.concat([self._unpack(value)] * 2, 'time')
            self._size = 1
        else:
            # Check if we need to expand the buffer
            if self._size >= self._buffer['data'].sizes['time']:
                # Double the buffer size by concatenating with itself
                self._buffer = sc.concat([self._buffer, self._buffer], 'time')

            # Insert the new value at the current size position
            self._buffer['data']['time', self._size] = value.data
            self._buffer['time']['time', self._size] = value.coords['time']
            self._size += 1

    def clear(self) -> None:
        self._size = 0


def _prepare_workflow(instrument: Instrument, monitor_name: str) -> sciline.Pipeline:
    workflow = GenericTofWorkflow(run_types=[CurrentRun], monitor_types=[CustomMonitor])
    workflow[Filename[CurrentRun]] = instrument.nexus_file
    workflow[NeXusName[CustomMonitor]] = monitor_name
    workflow.insert(_get_interval)
    return workflow


# TODO
# remove source_to_key
# add wrappers in all instruments
# Can the wrapper be created once per instrument, then create wrappings once per processor?


class WorkflowKeyAdapter(streaming.StreamProcessor):
    def __init__(self, dynamic_key_map: dict[Hashable, sciline.typing.Key], **kwargs):
        super().__init__(**kwargs)
        self._dynamic_key_map = dynamic_key_map

    def accumulate(self, chunks: dict[Hashable, Any]) -> None:
        super().accumulate(
            {self._dynamic_key_map.get(k, k): v for k, v in chunks.items()}
        )


def register_monitor_timeseries_workflows(
    instrument: Instrument, source_names: list[str]
) -> None:
    """Register monitor timeseries workflows for the given instrument and source names.

    Parameters
    ----------
    instrument
        The instrument for which to register the workflows.
    source_names
        The source names (monitor names) for which to register the workflows.
    """
    # Auto-gen the param model?
    # Or just use XEdges and YEdges? What about unit?
    # Current WorkflowSpec cannot deal with aux-source-name selector. Can it be extended
    # or can we use a factory instead, to make them on the fly?
    # Can we extend WorkflowConfig?
    # Or can we pass aux_source_names in the params?
    # No, pass it in WorkflowConfig?
    # "instantiate with WorkflowSpec with these source names"?
    #
    # Ok, but how to we get/display the unit in the UI?

    @instrument.register_workflow(
        name='monitor_interval_timeseries',
        version=1,
        title='Monitor Interval Timeseries',
        description='Timeseries of counts in a monitor within a specified '
        'time-of-arrival range.',
        source_names=source_names,
        aux_source_names=[],
    )
    def monitor_timeseries_workflow(
        source_name: str, params: MonitorTimeseriesParams
    ) -> WorkflowKeyAdapter:
        wf = _prepare_workflow(instrument, monitor_name=source_name)
        wf[parameter_models.TOARange] = params.toa_range
        return WorkflowKeyAdapter(
            base_workflow=wf,
            dynamic_key_map={source_name: NeXusData[CustomMonitor, CurrentRun]},
            dynamic_keys=(NeXusData[CustomMonitor, CurrentRun],),
            context_keys=(),  # the logs to plot against
            target_keys=(MonitorCountsInInterval,),
            accumulators={MonitorCountsInInterval: TimeseriesAccumulator},
        )
