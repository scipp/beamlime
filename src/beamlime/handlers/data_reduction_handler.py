# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging

import scipp as sc

from beamlime.config.instrument import Instrument

from ..core.handler import Accumulator, HandlerFactory
from ..core.message import StreamId, StreamKind

# Registers monitor data workflows
from . import monitor_data_handler  # noqa: F401
from .accumulators import Cumulative, DetectorEvents
from .to_nxevent_data import ToNXevent_data
from .to_nxlog import ToNXlog


class ReductionHandlerFactory(
    HandlerFactory[DetectorEvents, sc.DataGroup[sc.DataArray]]
):
    """
    Factory for data reduction handlers.
    """

    def __init__(
        self, *, instrument: Instrument, logger: logging.Logger | None = None
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._instrument = instrument

    @property
    def instrument(self) -> Instrument:
        return self._instrument

    def make_preprocessor(self, key: StreamId) -> Accumulator | None:
        match key.kind:
            case StreamKind.MONITOR_COUNTS:
                return Cumulative(clear_on_get=True)
            case StreamKind.LOG:
                attrs = self._instrument.f144_attribute_registry[key.name]
                return ToNXlog(attrs=attrs)
            case StreamKind.MONITOR_EVENTS | StreamKind.DETECTOR_EVENTS:
                return ToNXevent_data()
            case _:
                return None
