# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging

import scipp as sc

from ..core.handler import (
    Config,
    ConfigRegistry,
    FakeConfigRegistry,
    HandlerFactory,
    PeriodicAccumulatingHandler,
)
from ..core.message import MessageKey, StreamKind
from .accumulators import Cumulative, MonitorEvents, TOAHistogrammer


def make_monitor_data_preprocessor(
    key: MessageKey, config: Config
) -> (
    PeriodicAccumulatingHandler[MonitorEvents, sc.DataArray]
    | PeriodicAccumulatingHandler[sc.DataArray, sc.DataArray]
):
    match key.kind:
        case StreamKind.MONITOR_EVENTS:
            return TOAHistogrammer(config=config)
        case StreamKind.MONITOR_COUNTS:
            return Cumulative(config=config, clear_on_get=True)
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
        self, key: MessageKey
    ) -> (
        PeriodicAccumulatingHandler[MonitorEvents, sc.DataArray]
        | PeriodicAccumulatingHandler[sc.DataArray, sc.DataArray]
    ):
        config = self._config_registry.get_config(key.source_name)
        accumulators = {
            'cumulative': Cumulative(config=config),
            'current': Cumulative(config=config, clear_on_get=True),
        }
        preprocessor = make_monitor_data_preprocessor(key, config)
        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=config,
            preprocessor=preprocessor,
            accumulators=accumulators,
        )
