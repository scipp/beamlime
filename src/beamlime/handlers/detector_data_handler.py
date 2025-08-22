# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import logging
import pathlib
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable
from dataclasses import dataclass, field
from typing import Any, Literal

import pydantic
import scipp as sc
import scippnexus as snx
from ess.reduce.live import raw

from .. import parameter_models
from ..config import models
from ..config.instrument import Instrument
from ..config.instruments import get_config
from ..core.handler import Accumulator, JobBasedHandlerFactoryBase
from ..core.message import StreamId, StreamKind
from .accumulators import DetectorEvents, GroupIntoPixels
from .stream_processor_factory import StreamProcessor

# TODO
# remove models.TOARange
# use parameter_models.TOARange
# remove ConfigModelAccessor
# remove PeriodicAccumulatingHandler?
# instrument name hacks. introduce workflow namespaces instead?
# accumulators[f'{name}/roi'] = ROIBasedTOAHistogram(
#     config=config, roi_filter=view.make_roi_filter()
# )


class DetectorViewParams(pydantic.BaseModel):
    pixel_weighting: models.PixelWeighting = pydantic.Field(
        title="Pixel Weighting",
        description="Whether to apply pixel weighting based on the number of pixels "
        "contributing to each screen pixel.",
        default=models.PixelWeighting(
            enabled=True, method=models.WeightingMethod.PIXEL_NUMBER
        ),
    )
    # TODO split out the enabled flag?
    toa_range: parameter_models.TOARange = pydantic.Field(
        title="Time of Arrival Range",
        description="Time of arrival range for detector data.",
        default=parameter_models.TOARange(),
    )


# Yes? No? Maybe?
class ProjectionParams(pydantic.BaseModel):
    resolution: int = pydantic.Field(
        title="Resolution",
        description="Resolution of the projection in pixels.",
        default=100,
    )
    pixel_noise: float | None = pydantic.Field(
        title="Pixel Noise",
        description="Pixel noise to be applied to the projection.",
        default=None,
    )


@dataclass(frozen=True, kw_only=True)
class ViewConfig(ABC):
    name: str
    title: str
    description: str


class DetectorProcessorFactory(ABC):
    def __init__(
        self, *, instrument: Instrument, config: ViewConfig, source_names: list[str]
    ) -> None:
        self._instrument = instrument.name[: -len('_detectors')]
        self._config = config
        self._source_names = source_names
        self._window_length = 1
        self._nexus_file = _try_get_nexus_geometry_filename(
            instrument.name[: -len('_detectors')]
        )
        self._register_with_instrument(instrument)

    def make_view(self, source_name: str, params: DetectorViewParams) -> DetectorView:
        return DetectorView(
            params=params, detector_view=self._make_rollingview(source_name)
        )

    def _register_with_instrument(self, instrument: Instrument) -> None:
        instrument.register_workflow(
            name=self._config.name,
            version=1,
            title=self._config.title,
            description=self._config.description,
            source_names=self._source_names,
        )(self.make_view)

    @abstractmethod
    def _make_rollingview(self, source_name: str) -> raw.RollingDetectorView:
        """Create a RollingDetectorView for the given source name."""


class DetectorProjection(DetectorProcessorFactory):
    def __init__(
        self,
        *,
        instrument: Instrument,
        projection: Literal['xy_plane', 'cylinder_mantle_z'],
        resolution: dict[str, dict[str, int]],
    ) -> None:
        self._projection = projection
        self._resolution = resolution
        if projection == 'xy_plane':
            config = ViewConfig(
                name='detector_xy_projection',
                title='Detector XY Projection',
                description='Projection of a detector bank onto an XY-plane.',
            )
        elif projection == 'cylinder_mantle_z':
            config = ViewConfig(
                name='detector_cylinder_mantle_z',
                title='Detector Cylinder Mantle Z Projection',
                description='Projection of a detector bank onto a cylinder mantle '
                'along Z-axis.',
            )
        else:
            raise ValueError(f'Unsupported projection: {projection}')

        super().__init__(
            instrument=instrument, config=config, source_names=list(resolution)
        )

    def _get_resolution(self, source_name: str) -> dict[str, int]:
        aspect = self._resolution[source_name]
        scale = 8
        return {key: value * scale for key, value in aspect.items()}

    def _make_rollingview(self, source_name: str) -> raw.RollingDetectorView:
        return raw.RollingDetectorView.from_nexus(
            self._nexus_file,
            detector_name=source_name,
            window=self._window_length,
            projection=self._projection,
            resolution=self._get_resolution(source_name),
            pixel_noise=sc.scalar(4.0, unit='mm'),
        )


@dataclass(frozen=True, kw_only=True)
class LogicalViewConfig(ViewConfig):
    # If no projection defined, the shape of the detector_number is used.
    transform: Callable[[sc.DataArray], sc.DataArray] | None = None
    _detector_numbers: dict[str, sc.Variable] = field(default_factory=dict)

    @property
    def source_names(self) -> list[str]:
        """Return the source names for the logical view."""
        return list(self._detector_numbers.keys())

    def add_detector(self, source_name: str, detector_number: sc.Variable) -> None:
        self._detector_numbers[source_name] = detector_number

    def get_detector_number(self, source_name: str) -> sc.Variable:
        """Get the detector number for the given source name."""
        return self._detector_numbers[source_name]


class DetectorLogicalView(DetectorProcessorFactory):
    def __init__(self, *, instrument: Instrument, config: LogicalViewConfig) -> None:
        super().__init__(
            instrument=instrument, config=config, source_names=config.source_names
        )
        self._config = config

    def _make_rollingview(self, source_name: str) -> raw.RollingDetectorView:
        return raw.RollingDetectorView(
            detector_number=self._config.get_detector_number(source_name),
            window=self._window_length,
            projection=self._config.transform,
        )


class ROIHistogramParams(pydantic.BaseModel):
    region_of_interest: None = None  # TODO
    toa_edges: parameter_models.TOAEdges = pydantic.Field(
        title="Time of Arrival Edges",
        description="Time of arrival edges for histogramming.",
        default=parameter_models.TOAEdges(
            start=0.0,
            stop=1000.0 / 14,
            num_bins=100,
            unit=parameter_models.TimeUnit.MS,
        ),
    )


class ROIHistogram(StreamProcessor):
    def __init__(self, source_name: str, params: ROIHistogramParams) -> None:
        pass


def make_detector_data_instrument(name: str) -> Instrument:
    """Create an Instrument for detector view workflows."""
    return Instrument(name=f'{name}_detectors')


class DetectorHandlerFactory(JobBasedHandlerFactoryBase[DetectorEvents, sc.DataArray]):
    """
    Factory for detector data handlers.

    Handlers are created based on the instrument name in the message key which should
    identify the detector name. Depending on the configured detector views a NeXus file
    with geometry information may be required to setup the view. Currently the NeXus
    files are always obtained via Pooch. Note that this may need to be refactored to a
    more dynamic approach in the future.
    """

    def __init__(
        self, *, instrument: Instrument, logger: logging.Logger | None = None
    ) -> None:
        super().__init__(instrument=instrument, logger=logger)
        self._instrument_name = instrument.name[: -len('_detectors')]
        self._detector_config = get_config(self._instrument_name).detectors_config[
            'detectors'
        ]
        self._nexus_file = _try_get_nexus_geometry_filename(self._instrument_name)
        self._window_length = 1

    # TODO cache
    def get_detector_number(self, detector_name: str) -> sc.Variable | None:
        for det_config in self._detector_config.values():
            if det_config['detector_name'] == detector_name:
                if (detector_number := det_config.get('detector_number')) is not None:
                    return detector_number
        if self._nexus_file is None:
            return None
        try:
            return snx.load(
                self._nexus_file,
                root=f'entry/instrument/{detector_name}/detector_number',
            )
        except (FileNotFoundError, KeyError):
            self._logger.error(
                'Could not find detector number for %s in NeXus file %s',
                detector_name,
                self._nexus_file,
            )
            return None

    def make_preprocessor(self, key: StreamId) -> Accumulator | None:
        match key.kind:
            case StreamKind.DETECTOR_EVENTS:
                if (detector_number := self.get_detector_number(key.name)) is None:
                    return None
                return GroupIntoPixels(detector_number=detector_number)
            case _:
                return None


class DetectorView(StreamProcessor):
    """
    Accumulator for detector counts, based on a rolling detector view.

    Return both a current (since last update) and a cumulative view of the counts.
    """

    def __init__(
        self,
        params: DetectorViewParams,
        detector_view: raw.RollingDetectorView,
    ) -> None:
        self._use_toa_range = params.toa_range.enabled
        self._toa_range = params.toa_range.range_ns
        self._use_weights = params.pixel_weighting.enabled
        # Note: Currently we use default weighting based on the number of detector
        # pixels contributing to each screen pixel. In the future more advanced options
        # such as by the signal of a uniform scattered may need to be supported.
        weighting = params.pixel_weighting
        if weighting.method != models.WeightingMethod.PIXEL_NUMBER:
            raise ValueError(f'Unsupported pixel weighting method: {weighting.method}')
        self._use_weights = weighting.enabled
        self._view = detector_view
        self._inv_weights = sc.reciprocal(detector_view.transform_weights())
        self._previous: sc.DataArray | None = None

    def apply_toa_range(self, data: sc.DataArray) -> sc.DataArray:
        if not self._use_toa_range:
            return data
        low, high = self._toa_range
        # GroupIntoPixels stores time-of-arrival as the data variable of the bins to
        # avoid allocating weights that are all ones. For filtering we need to turn this
        # into a coordinate, since scipp does not support filtering on data variables.
        return data.bins.assign_coords(toa=data.bins.data).bins['toa', low:high]

    def accumulate(self, data: dict[Hashable, Any]) -> None:
        """
        Add data to the accumulator.

        Parameters
        ----------
        data:
            Data to be added. It is assumed that this is ev44 data that was passed
            through :py:class:`GroupIntoPixels`.
        """
        if len(data) != 1:
            raise ValueError("DetectorViewProcessor expects exactly one data item.")
        raw = next(iter(data.values()))
        filtered = self.apply_toa_range(raw)
        self._view.add_events(filtered)

    def finalize(self) -> dict[str, sc.DataArray]:
        cumulative = self._view.cumulative.copy()
        # This is a hack to get the current counts. Should be updated once
        # ess.reduce.live.raw.RollingDetectorView has been modified to support this.
        current = cumulative
        if self._previous is not None:
            current = current - self._previous
        self._previous = cumulative
        result = sc.DataGroup(cumulative=cumulative, current=current)
        return dict(result * self._inv_weights if self._use_weights else result)

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
    'geometry-dream-2025-05-01.nxs': 'md5:773fc7e84d0736a0121818cbacc0697f',
    'geometry-dream-no-shape-2025-05-01.nxs': 'md5:4471e2490a3dd7f6e3ed4aa0a1e0b47d',
    'geometry-loki-2025-01-01.nxs': 'md5:8d0e103276934a20ba26bb525e53924a',
    'geometry-loki-2025-03-26.nxs': 'md5:279dc8cf7dae1fac030d724bc45a2572',
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
