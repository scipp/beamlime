# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import numpy as np
import pydantic
import scipp as sc

from ess.livedata.config.workflow_spec import JobId, JobNumber, ResultKey, WorkflowId
from ess.livedata.parameter_models import EdgesModel, make_edges

from .data_service import DataService
from .stream_manager import StreamManager
from .widgets.configuration_widget import ConfigurationAdapter


class EdgesWithUnit(EdgesModel):
    # Frozen so it cannot be changed in the UI. If we wanted to auto-generate an enum
    # to get a dropdown menu, we probably cannot write generic code below, since we
    # would need to create all the models on the fly.
    unit: str = pydantic.Field(..., description="Unit for the edges", frozen=True)

    def make_edges(self, dim: str) -> sc.Variable:
        return make_edges(model=self, dim=dim, unit=self.unit)


def _make_edges_field(name: str, coord: sc.DataArray) -> Any:
    unit = str(coord.unit)
    low = coord.nanmin().value
    high = np.nextafter(coord.nanmax().value, np.inf)
    return pydantic.Field(
        default=EdgesWithUnit(start=low, stop=high, num_bins=50, unit=unit),
        title=f"{name} bins",
        description=f"Define the bin edges for histogramming in {name}.",
    )


class CorrelationHistogramParams(pydantic.BaseModel):
    _x_dim: str
    _y_dim: str | None

    def get_all_edges(self) -> dict[str, sc.Variable]:
        if (x_edges := getattr(self, 'x_edges', None)) is None:
            raise ValueError("x_edges is not set.")
        edges = {self._x_dim: x_edges.make_edges(dim=self._x_dim)}
        if self._y_dim is None:
            return edges
        if (y_edges := getattr(self, 'y_edges', None)) is None:
            raise ValueError("y_edges is not set.")
        edges[self._y_dim] = y_edges.make_edges(dim=self._y_dim)
        return edges


def make_params(coords: dict[str, sc.DataArray]) -> type[CorrelationHistogramParams]:
    fields = [(name, _make_edges_field(name, coord)) for name, coord in coords.items()]
    if not (0 < len(fields) < 3):
        raise ValueError("Expected 1 or 2 coordinates for correlation histogram.")

    x_dim, x_field = fields[0]

    class CorrelationHistogram1dParams(CorrelationHistogramParams):
        # TODO Add start_time, normalize
        _x_dim: str = x_dim
        x_edges: EdgesWithUnit = x_field

    if len(fields) == 1:
        return CorrelationHistogram1dParams

    y_dim, y_field = fields[1]

    class CorrelationHistogram2dParams(CorrelationHistogram1dParams):
        _y_dim: str | None = y_dim
        y_edges: EdgesWithUnit = y_field

    return CorrelationHistogram2dParams


class CorrelationHistogramConfigurationAdapter(
    ConfigurationAdapter[CorrelationHistogramParams]
):
    def __init__(
        self,
        *,
        title: str,
        description: str,
        model_class: type[CorrelationHistogramParams],
        result_keys: list[ResultKey],
        axis_keys: list[ResultKey],
        initial_parameter_values: dict[str, pydantic.BaseModel] | None = None,
        start_action: Callable[
            [list[ResultKey], list[ResultKey], CorrelationHistogramParams], None
        ],
    ) -> None:
        self._title = title
        self._description = description
        self._model_class = model_class
        self._source_names = {
            result_key.job_id.source_name: result_key for result_key in result_keys
        }
        self._axis_keys = axis_keys
        self._initial_parameter_values = initial_parameter_values or {}
        self._start_action = start_action

    @property
    def title(self) -> str:
        return self._title

    @property
    def description(self) -> str:
        return self._description

    @property
    def model_class(self) -> type[CorrelationHistogramParams]:
        return self._model_class

    @property
    def source_names(self) -> list[str]:
        return list(self._source_names)

    @property
    def initial_source_names(self) -> list[str]:
        # In practice we may have many timeseries. We do not want to auto-populate the
        # selection since typically the user will want to select just one or a few.
        return []

    @property
    def initial_parameter_values(self) -> dict[str, Any]:
        return self._initial_parameter_values

    def start_action(
        self, selected_sources: list[str], parameter_values: CorrelationHistogramParams
    ) -> bool:
        selected_result_keys = [
            self._source_names[source_name] for source_name in selected_sources
        ]
        self._start_action(
            data_keys=selected_result_keys,
            axis_keys=self._axis_keys,
            params=parameter_values,
        )
        return True


class CorrelationHistogramController:
    def __init__(self, data_service: DataService[ResultKey, sc.DataArray]) -> None:
        self._data_service = data_service
        self._update_subscribers: list[Callable[[], None]] = []
        self._data_service.subscribe_to_changed_keys(self._on_data_keys_updated)
        self._pipes: list[Any] = []

    def register_update_subscriber(self, callback: Callable[[], None]) -> None:
        """Register a callback to be called when job data is updated."""
        self._update_subscribers.append(callback)

    def _on_data_keys_updated(
        self, added: set[ResultKey], removed: set[ResultKey]
    ) -> None:
        """Handle job updates from the job service."""
        _ = added
        _ = removed
        for callback in self._update_subscribers:
            callback()

    def get_timeseries(self) -> list[ResultKey]:
        return [key for key, da in self._data_service.items() if _is_timeseries(da)]

    def create_config(
        self, axis_keys: list[ResultKey]
    ) -> CorrelationHistogramConfigurationAdapter:
        result_keys = [key for key in self.get_timeseries() if key not in axis_keys]
        coords = {key.job_id.source_name: self._data_service[key] for key in axis_keys}
        ndim = len(coords)
        return CorrelationHistogramConfigurationAdapter(
            title=f"{ndim}D Correlation Histogram",
            description=f"Configure parameters for a {ndim}D correlation histogram.",
            model_class=make_params(coords),
            result_keys=result_keys,
            axis_keys=axis_keys,
            start_action=self.start_workflows,
        )

    def start_workflows(
        self,
        data_keys: list[ResultKey],
        axis_keys: list[ResultKey],
        params: CorrelationHistogramParams,
    ) -> None:
        # TODO JobStatus reporting? How to stop?
        data = {key: self._data_service[key] for key in data_keys}
        axes = {key: self._data_service[key] for key in axis_keys}
        job_number = uuid.uuid4()  # New unique job number shared by all workflows
        for key, value in data.items():
            pipe_factory = make_correlation_histogrammer_pipe_factory(
                data_key=key,
                coord_keys=axis_keys,
                params=params,
                job_number=job_number,
                data_service=self._data_service,
            )
            stream_manager = StreamManager(
                data_service=self._data_service, pipe_factory=pipe_factory
            )
            pipe = stream_manager.make_merging_stream({key: value, **axes})
            self._pipes.append(pipe)  # Keep a reference to avoid garbage collection


def _is_timeseries(da: sc.DataArray) -> bool:
    return da.dims == ('time',) and 'time' in da.coords


def make_correlation_histogrammer_pipe_factory(
    data_key: ResultKey,
    coord_keys: list[ResultKey],
    params: CorrelationHistogramParams,
    job_number: JobNumber,
    data_service: DataService[ResultKey, sc.DataArray],
) -> Any:
    class CorrelationHistogrammerPipe:
        """Connector of Pipe expected by StreamManager to CorrelationHistogrammer."""

        def __init__(self, data: dict[ResultKey, sc.DataArray]) -> None:
            self._data_key = data_key
            self._data_service = data_service
            self._histogrammer = CorrelationHistogrammer(edges=params.get_all_edges())
            self._coords = {key: key.job_id.source_name for key in coord_keys}
            self._result_key = ResultKey(
                workflow_id=WorkflowId(
                    instrument=data_key.workflow_id.instrument,
                    namespace='correlation',
                    name=f'correlation_histogram_{"_".join(self._coords.values())}',
                    version=1,
                ),
                job_id=JobId(
                    source_name=data_key.job_id.source_name, job_number=job_number
                ),
                output_name=None,
            )
            self.send(data)

        def send(self, data: dict[ResultKey, sc.DataArray]) -> None:
            coords = {name: data[key] for key, name in self._coords.items()}
            result = self._histogrammer(data[self._data_key], coords=coords)
            # TODO Ensure still in transaction, or fix DataService
            self._data_service[self._result_key] = result

    return CorrelationHistogrammerPipe


class CorrelationHistogrammer:
    def __init__(self, edges: dict[str, sc.Variable]) -> None:
        self._edges = edges

    def __call__(
        self, data: sc.DataArray, coords: dict[str, sc.DataArray]
    ) -> sc.DataArray:
        dependent = data.copy(deep=False)
        # Note that this implementation is naive and inefficient as timeseries grow.
        # An alternative approach, streaming only the new data and directly updating
        # only the target bin may need to be considered in the future. This would have
        # the downside of not being able to recreate the histogram for past data though,
        # unless we replay the Kafka topic.
        # For now, if we expect timeseries to not update more than once per second, this
        # should be acceptable.
        for dim in self._edges:
            lut = sc.lookup(sc.values(coords[dim]), mode='previous')
            dependent.coords[dim] = lut[dependent.coords['time']]
        return dependent.hist(**self._edges)
