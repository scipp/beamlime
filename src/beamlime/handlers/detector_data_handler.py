# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
import pathlib
import re
from typing import Any

import scipp as sc
from ess.reduce.live import raw

from ..config import models
from ..config.raw_detectors import get_config
from ..core.handler import (
    Accumulator,
    Config,
    ConfigModelAccessor,
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
        self._instrument = instrument
        self._detector_config = get_config(instrument).detectors_config['detectors']
        self._nexus_file = _try_get_nexus_geometry_filename(instrument)
        self._window_length = 1

    def _key_to_detector_name(self, key: MessageKey) -> str:
        return key.source_name

    def _make_view(
        self, detector_config: dict[str, Any]
    ) -> raw.RollingDetectorView | None:
        projection = detector_config.get('projection')
        if (
            (self._nexus_file is None or self._instrument == 'bifrost')
            and (projection is None or not isinstance(projection, str))
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
        detector_number: sc.Variable | None = None
        accumulators: dict[str, Accumulator[sc.DataArray, sc.DataArray]] = {}
        for name, view in views.items():
            detector_number = view.detector_number
            accumulators[name] = DetectorCounts(
                logger=self._logger, config=self._config, detector_view=view
            )
            accumulators[f'{name}/roi'] = ROIBasedTOAHistogram(
                config=self._config, roi_filter=view.make_roi_filter()
            )
        if detector_number is None:
            preprocessor = NullAccumulator()
        else:
            preprocessor = GroupIntoPixels(
                config=self._config, detector_number=detector_number
            )

        return PeriodicAccumulatingHandler(
            logger=self._logger,
            config=self._config,
            preprocessor=preprocessor,
            accumulators=accumulators,
        )


class DetectorCounts(Accumulator[sc.DataArray, sc.DataGroup[sc.DataArray]]):
    """
    Accumulator for detector counts, based on a rolling detector view.

    Return both a current (since last update) and a cumulative view of the counts.
    """

    def __init__(
        self,
        logger: logging.Logger,
        config: Config,
        detector_view: raw.RollingDetectorView,
    ):
        self._logger = logger
        self._view = detector_view
        self._inv_weights = sc.reciprocal(detector_view.transform_weights())
        self._toa_range = ConfigModelAccessor(
            config, 'toa_range', model=models.TOARange, convert=self._convert_toa_range
        )
        self._use_weights = ConfigModelAccessor(
            config,
            'pixel_weighting',
            model=models.PixelWeighting,
            convert=self._convert_pixel_weighting,
        )
        self._previous: sc.DataArray | None = None

    def _convert_toa_range(
        self, value: dict[str, Any]
    ) -> tuple[sc.Variable, sc.Variable] | None:
        model = models.TOARange.model_validate(value)
        self.clear()
        return model.range_ns

    def _convert_pixel_weighting(self, value: dict[str, Any]) -> bool:
        model = models.PixelWeighting.model_validate(value)
        # Note: Currently we use default weighting based on the number of detector
        # pixels contributing to each screen pixel. In the future more advanced options
        # such as by the signal of a uniform scattered may need to be supported.
        if model.method != models.WeightingMethod.PIXEL_NUMBER:
            raise ValueError(f'Unsupported pixel weighting method: {model.method}')
        return model.enabled

    def apply_toa_range(self, data: sc.DataArray) -> sc.DataArray:
        if (toa_range := self._toa_range()) is None:
            return data
        low, high = toa_range
        # GroupIntoPixels stores time-of-arrival as the data variable of the bins to
        # avoid allocating weights that are all ones. For filtering we need to turn this
        # into a coordinate, since scipp does not support filtering on data variables.
        return data.bins.assign_coords(toa=data.bins.data).bins['toa', low:high]

    def add(self, timestamp: int, data: sc.DataArray) -> None:
        """
        Add data to the accumulator.

        Parameters
        ----------
        timestamp:
            Timestamp of the data.
        data:
            Data to be added. It is assumed that this is ev44 data that was passed
            through :py:class:`GroupIntoPixels`.
        """
        data = self.apply_toa_range(data)
        self._view.add_events(data)

    def get(self) -> sc.DataGroup[sc.DataArray]:
        cumulative = self._view.cumulative.copy()
        # This is a hack to get the current counts. Should be updated once
        # ess.reduce.live.raw.RollingDetectorView has been modified to support this.
        current = cumulative
        if self._previous is not None:
            current = current - self._previous
        self._previous = cumulative
        result = sc.DataGroup(cumulative=cumulative, current=current)
        return result * self._inv_weights if self._use_weights() else result

    def clear(self) -> None:
        self._view.clear_counts()
        self._previous = None


# Note: Currently no need for a geometry file for NMX since the view is purely logical.
# DetectorHandlerFactory will fall back to use the detector_number configured in the
# detector view config.
# Note: There will be multiple files per instrument, valid for different date ranges.
# Files should thus not be replaced by making use of the pooch versioning mechanism.
_registry = {
    'geometry-dream-2025-01-01.nxs': 'md5:91aceb884943c76c0c21400ee74ad9b6',
    'geometry-loki-2025-01-01.nxs': 'md5:8d0e103276934a20ba26bb525e53924a',
    'geometry-bifrost-2025-01-01.nxs': 'md5:ae3caa99dd56de9495b9321eea4e4fef',
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
