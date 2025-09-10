from typing import Any

import numpy as np
import scipp as sc

from ess.livedata.core.handler import Accumulator
from ess.livedata.handlers.accumulators import LogData


class ToNXlog(Accumulator[LogData, sc.DataArray]):
    """
    Preprocessor for log data.

    Accumulates LogData objects and returns a single DataArray as it would be read from
    an NXlog in a NeXus file. The DataArray grows as data is added and is not cleared
    until explicitly requested.
    """

    def __init__(self, attrs: dict[str, Any]) -> None:
        self._attrs = attrs
        # Values with no unit are ok
        maybe_unit = self._attrs.get('units')
        if maybe_unit is None:
            self._unit = None
        else:
            self._unit = sc.Unit(maybe_unit)
        # Hard-coded time unit and start in the ESS NeXus filewriter
        self._time_unit = 'ns'
        self._start = sc.epoch(unit='ns')

        # Initialize with None, will be created on first add
        self._timeseries: sc.DataArray | None = None
        self._end = 0

    @property
    def unit(self) -> str | None:
        return self._unit

    def _at_capacity(self) -> bool:
        return self._end >= self._timeseries.sizes['time']

    def _ensure_capacity(self, data) -> None:
        if self._timeseries is None:
            # Initialize with initial capacity of 2
            arr = np.asarray(data)
            values = sc.zeros(
                dims=['time'], shape=[2, *arr.shape], unit=self._unit, dtype=arr.dtype
            )
            times = sc.zeros(
                dims=['time'], shape=[2], unit=self._time_unit, dtype='int64'
            )
            self._timeseries = sc.DataArray(
                values, coords={'time': self._start + times}
            )
        elif self._at_capacity():
            # Double capacity when full
            self._timeseries = sc.concat(
                [self._timeseries, self._timeseries], dim='time'
            )

    def add(self, timestamp: int, data: LogData) -> None:
        self._ensure_capacity(data.value)
        self._timeseries.coords['time'].values[self._end] = data.time
        self._timeseries.data.values[self._end] = data.value
        self._end += 1

    def get(self) -> sc.DataArray:
        if self._timeseries is None or self._end == 0:
            # Return empty DataArray with correct structure if no data
            values = sc.array(dims=['time'], values=[], unit=self._unit)
            times = sc.array(
                dims=['time'], values=[], unit=self._time_unit, dtype='int64'
            )
            return sc.DataArray(values, coords={'time': self._start + times})

        # Return only the filled part and sort by time
        result = self._timeseries['time', : self._end]
        return sc.sort(result, 'time') if self._end > 1 else result

    def clear(self) -> None:
        self._end = 0
        # Keep the allocated array to avoid reallocations
