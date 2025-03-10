from typing import Any

import scipp as sc
from scippnexus.field import _as_datetime as snx_as_datetime

from beamlime.core.handler import Accumulator
from beamlime.handlers.accumulators import LogData


class ToNXlog(Accumulator[LogData, sc.DataArray]):
    """
    Preprocessor for log data.

    Concatenates LogData objects and returns a single DataArray as it would be read from
    and NXlog in a NeXus file.
    """

    def __init__(self, attrs: dict[str, Any]) -> None:
        self._attrs = attrs
        self._values: list[LogData] = []
        # Values with no unit are ok
        self._unit = self._attrs.get('value', {}).get('units')
        # Time must always have a unit
        self._time_unit = self._attrs['time']['units']
        start = snx_as_datetime(self._attrs['time']['start'])
        if start is None:
            raise ValueError(
                f'Failed to parse start time {self._attrs["time"]["start"]}'
            )
        self._start = start.to(unit=self._time_unit)

    @property
    def unit(self) -> str | None:
        return self._unit

    def add(self, timestamp: int, data: LogData) -> None:
        self._values.append(data)

    def get(self) -> sc.DataArray:
        values = [data.value for data in self._values]
        times = sc.array(
            dims=['time'],
            values=[data.time for data in self._values],
            unit=self._time_unit,
            dtype='int64',
        )
        self.clear()
        result = sc.DataArray(
            sc.array(dims=['time'], values=values, unit=self._unit),
            coords={'time': self._start + times},
        )
        return sc.sort(result, 'time') if len(values) > 1 else result

    def clear(self) -> None:
        self._values.clear()
