# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging

import numpy as np
import scipp as sc
from ess.reduce.live import raw

from ..config.raw_detectors import (
    dream_detectors_config,
    loki_detectors_config,
    nmx_detectors_config,
)
from ..core.handler import Accumulator, Config, Handler, PeriodicAccumulatingHandler
from ..core.message import MessageKey
from .accumulators import PixelIDMerger

detector_registry = {
    'dream': dream_detectors_config,
    'loki': loki_detectors_config,
    'nmx': nmx_detectors_config,
}


class DetectorHandlerRegistry:
    def __init__(
        self, *, instrument: str, logger: logging.Logger | None = None, config: Config
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._handlers: dict[MessageKey, Handler] = {}
        self._detector_config = detector_registry[instrument]['detectors']
        self._nexus_file = (
            '/home/simon/instruments/dream/443503_00033178.hdf'  # New Dream
        )
        self._window_length = 100

    def _key_to_detector_name(self, key: MessageKey) -> str:
        return key.source_name

    def _make_handler(self, key: MessageKey) -> Handler:
        detector = self._detector_config[self._key_to_detector_name(key)]
        detector_view = raw.RollingDetectorView.from_nexus(
            self._nexus_file,
            detector_name=detector['detector_name'],
            window=self._window_length,
            projection=detector['projection'],
            resolution=detector.get('resolution'),
            pixel_noise=detector.get('pixel_noise'),
        )
        accumulators = {
            'sliding': DetectorCounts(config=self._config, detector_view=detector_view)
        }
        preprocessor = PixelIDMerger(config=self._config)
        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=self._config,
            preprocessor=preprocessor,
            accumulators=accumulators,
        )

    def get(self, key: MessageKey) -> Handler:
        if key not in self._handlers:
            self._handlers[key] = self._make_handler(key)
        return self._handlers[key]


class DetectorCounts(Accumulator[np.array, sc.DataArray]):
    def __init__(self, config: Config, detector_view: raw.RollingDetectorView):
        self._config = config
        self._det = detector_view

    def add(self, timestamp: int, data: np.ndarray) -> None:
        _ = timestamp
        self._det.add_counts(data)

    def get(self) -> sc.DataArray:
        return self._det.get()

    def clear(self) -> None:
        self._det.clear_counts()
