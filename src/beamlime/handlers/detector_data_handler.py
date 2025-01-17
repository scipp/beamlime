# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import scipp as sc
from ess.reduce.live import raw

from ..config.raw_detectors import (
    dream_detectors_config,
    dummy_detectors_config,
    loki_detectors_config,
    nmx_detectors_config,
)
from ..core.handler import (
    Accumulator,
    Config,
    Handler,
    HandlerFactory,
    PeriodicAccumulatingHandler,
)
from ..core.message import MessageKey
from .accumulators import DetectorEvents, PixelIDMerger

detector_registry = {
    'dummy': dummy_detectors_config,
    'dream': dream_detectors_config,
    'loki': loki_detectors_config,
    'nmx': nmx_detectors_config,
}


class DetectorHandlerFactory(HandlerFactory[DetectorEvents, sc.DataArray]):
    def __init__(
        self,
        *,
        instrument: str,
        nexus_file: str | None,
        logger: logging.Logger | None = None,
        config: Config,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._handlers: dict[MessageKey, Handler] = {}
        self._detector_config = detector_registry[instrument]['detectors']
        self._nexus_file = nexus_file
        self._window_length = 10

    def _key_to_detector_name(self, key: MessageKey) -> str:
        return key.source_name

    def _make_view(
        self, detector_config: dict[str, Any]
    ) -> raw.RollingDetectorView | None:
        projection = detector_config['projection']
        if (
            self._nexus_file is None
            and isinstance(projection, raw.LogicalView)
            and (detector_number := detector_config.get('detector_number')) is not None
        ):
            return raw.RollingDetectorView(
                detector_number=detector_number,
                window=self._window_length,
                projection=projection,
            )
        if self._nexus_file is None:
            self._logger.error(
                'NeXus file is required to setup detector view for %s', detector_config
            )
            return None
        return raw.RollingDetectorView.from_nexus(
            self._nexus_file,
            detector_name=detector_config['detector_name'],
            window=self._window_length,
            projection=detector_config['projection'],
            resolution=detector_config.get('resolution'),
            pixel_noise=detector_config.get('pixel_noise'),
        )

    def make_handler(self, key: MessageKey) -> Handler[DetectorEvents, sc.DataArray]:
        detector_name = self._key_to_detector_name(key)
        views = {
            view_name: self._make_view(detector)
            for view_name, detector in self._detector_config.items()
            if detector['detector_name'] == detector_name
        }
        views = {name: view for name, view in views.items() if view is not None}
        if not views:
            self._logger.warning('No views configured for %s', detector_name)
        accumulators = {
            f'sliding_{name}': DetectorCounts(config=self._config, detector_view=view)
            for name, view in views.items()
        }
        preprocessor = PixelIDMerger(config=self._config)
        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=self._config,
            preprocessor=preprocessor,
            accumulators=accumulators,
        )


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
