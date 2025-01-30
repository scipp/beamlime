# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
import pathlib
import re
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
from .accumulators import (
    DetectorEvents,
    GroupIntoPixels,
    NullAccumulator,
    ROIBasedTOAHistogram,
)

detector_registry = {
    'dummy': dummy_detectors_config,
    'dream': dream_detectors_config,
    'loki': loki_detectors_config,
    'nmx': nmx_detectors_config,
}


class DetectorHandlerFactory(HandlerFactory[DetectorEvents, sc.DataArray]):
    """
    Factory for detector data handlers.

    Handlers are created based on the instrument name in the message key which should
    identify the detector name. Depending on the configured detector views a NeXus file
    with geometry information may be required to setup the view. Currently the NeXus
    files are always obtained via Pooch. Note that this may need to be refactored to a
    more dynamic approach in the future.
    """

    def __init__(
        self,
        *,
        instrument: str,
        logger: logging.Logger | None = None,
        config: Config,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._config = config
        self._detector_config = detector_registry[instrument]['detectors']
        self._nexus_file = _try_get_nexus_geometry_filename(instrument)
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
        candidates = {
            view_name: self._make_view(detector)
            for view_name, detector in self._detector_config.items()
            if detector['detector_name'] == detector_name
        }
        views = {name: view for name, view in candidates.items() if view is not None}
        if not views:
            self._logger.warning('No views configured for %s', detector_name)
        accumulators = {
            f'sliding_{name}': DetectorCounts(config=self._config, detector_view=view)
            for name, view in views.items()
        }
        if not accumulators:
            preprocessor = NullAccumulator()
        else:
            detector_number = next(iter(views.values()))._flat_detector_number
            preprocessor = GroupIntoPixels(
                config=self._config, detector_number=detector_number
            )
            accumulators['roi_histogram'] = ROIBasedTOAHistogram(config=self._config)
        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=self._config,
            preprocessor=preprocessor,
            accumulators=accumulators,
        )


class DetectorCounts(Accumulator[np.ndarray, sc.DataArray]):
    """Accumulator for detector counts, based on a rolling detector view."""

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


# Note: Currently no need for a geometry file for NMX since the view is purely logical.
# DetectorHandlerFactory will fall back to use the detector_number configured in the
# detector view config.
# Note: There will be multiple files per instrument, valid for different date ranges.
# Files should thus not be replaced by making use of the pooch versioning mechanism.
_registry = {
    'geometry-dream-2025-01-01.nxs': 'md5:91aceb884943c76c0c21400ee74ad9b6',
    'geometry-loki-2025-01-01.nxs': 'md5:8d0e103276934a20ba26bb525e53924a',
}


def _make_pooch():
    import pooch

    return pooch.create(
        path=pooch.os_cache('beamlime'),
        env='BEAMLIME_DATA_DIR',
        retry_if_failed=3,
        base_url='https://public.esss.dk/groups/scipp/beamlime/geometry/',
        version='0',
        registry=_registry,
    )


def _parse_filename_lut(instrument: str) -> sc.DataArray:
    """
    Returns a scipp DataArray with datetime index and filename values.
    """
    registry = [name for name in _registry if instrument in name]
    if not registry:
        raise ValueError(f'No geometry files found for instrument {instrument}')
    pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
    dates = [
        pattern.search(entry).group(1) for entry in registry if pattern.search(entry)
    ]
    datetimes = sc.datetimes(dims=['datetime'], values=[*dates, '9999-12-31'], unit='s')
    return sc.DataArray(
        sc.array(dims=['datetime'], values=registry), coords={'datetime': datetimes}
    )


def get_nexus_geometry_filename(
    instrument: str, date: sc.Variable | None = None
) -> pathlib.Path:
    """
    Get filename for NeXus file based on instrument and date.

    The file is fetched and cached with Pooch.
    """
    _pooch = _make_pooch()
    dt = (date if date is not None else sc.datetime('now')).to(unit='s')
    try:
        filename = _parse_filename_lut(instrument)['datetime', dt].value
    except IndexError:
        raise ValueError(f'No geometry file found for given date {date}') from None
    return pathlib.Path(_pooch.fetch(filename))


def _try_get_nexus_geometry_filename(
    instrument: str, date: sc.Variable | None = None
) -> pathlib.Path | None:
    try:
        return get_nexus_geometry_filename(instrument, date)
    except ValueError:
        return None
