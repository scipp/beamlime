# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import TypeVar

import scipp as sc

from ..core.handler import (
    Accumulator,
    Config,
    PeriodicAccumulatingHandler,
)
from .accumulators import Cumulative, Histogrammer, MonitorEvents, SlidingWindow


def create_monitor_event_data_handler(
    *, logger: logging.Logger | None = None, config: Config
) -> PeriodicAccumulatingHandler[MonitorEvents, sc.DataArray]:
    return _create_monitor_handler(
        logger=logger, config=config, preprocessor=Histogrammer(config=config)
    )


def create_monitor_data_handler(
    *, logger: logging.Logger | None = None, config: Config
) -> PeriodicAccumulatingHandler[sc.DataArray, sc.DataArray]:
    return _create_monitor_handler(
        logger=logger,
        config=config,
        preprocessor=Cumulative(config=config, clear_on_get=True),
    )


T = TypeVar('T')


def _create_monitor_handler(
    *,
    logger: logging.Logger | None = None,
    config: Config,
    preprocessor: Accumulator[T, sc.DataArray],
) -> PeriodicAccumulatingHandler[T, sc.DataArray]:
    accumulators = {
        'cumulative': Cumulative(config=config),
        'sliding': SlidingWindow(config=config),
    }
    return PeriodicAccumulatingHandler(
        logger=logger,
        config=config,
        preprocessor=preprocessor,
        accumulators=accumulators,
    )
