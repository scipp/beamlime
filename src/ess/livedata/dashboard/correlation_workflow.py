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


def _make_edges_field(name: str, coord: sc.DataArray) -> Any:
    unit = str(coord.unit)
    low = coord.nanmin().value
    high = np.nextafter(coord.nanmax().value, np.inf)
    return pydantic.Field(
        default=EdgesWithUnit(start=low, stop=high, num_bins=50, unit=unit),
        title=f"{name} Edges",
        description=f"Bin edges for the {name} axis",
    )


def make_params(coords: dict[str, sc.DataArray]) -> type[pydantic.BaseModel]:
    fields = [_make_edges_field(name, coord) for name, coord in coords.items()]
    if not (0 < len(fields) < 3):
        raise ValueError("Expected 1 or 2 coordinates for correlation histogram.")

    class CorrelationHistogram1dParams(pydantic.BaseModel):
        # TODO Add start_time, normalize
        x_edges: EdgesWithUnit = fields[0]

    if len(fields) == 1:
        return CorrelationHistogram1dParams

    class CorrelationHistogram2dParams(CorrelationHistogram1dParams):
        y_edges: EdgesWithUnit = fields[1]

    return CorrelationHistogram2dParams


class CorrelationHistogramConfigurationAdapter(ConfigurationAdapter):
    def __init__(
        self,
        *,
        title: str,
        description: str,
        model_class: type[pydantic.BaseModel],
        result_keys: list[ResultKey],
        axis_keys: list[ResultKey],
        initial_parameter_values: dict[str, pydantic.BaseModel] | None = None,
        start_action: Callable[
            [list[ResultKey], list[ResultKey], pydantic.BaseModel], None
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
    def model_class(self) -> type[pydantic.BaseModel]:
        return self._model_class

    @property
    def source_names(self) -> list[str]:
        return list(self._source_names)

    @property
    def initial_source_names(self) -> list[str]:
        return self.source_names

    @property
    def initial_parameter_values(self) -> dict[str, Any]:
        return self._initial_parameter_values

    def start_action(
        self, selected_sources: list[str], parameter_values: pydantic.BaseModel
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
        params: pydantic.BaseModel,
    ) -> None:
        # TODO JobStatus reporting? How to stop?
        data = {key: self._data_service[key] for key in data_keys}
        axes = {key: self._data_service[key] for key in axis_keys}
        job_number = uuid.uuid4()  # New unique job number shared by all workflows
        for key, value in data.items():
            builder = CorrelationHistogrammerBuilder(
                data_key=key,
                coord_keys=axis_keys,
                params=params,
                job_number=job_number,
                data_service=self._data_service,
            )
            stream_manager = StreamManager(
                data_service=self._data_service, pipe_factory=builder.create_pipe
            )
            pipe = stream_manager.make_merging_stream({key: value, **axes})
            self._pipes.append(pipe)  # Keep a reference to avoid garbage collection


def _is_timeseries(da: sc.DataArray) -> bool:
    return da.dims == ('time',) and 'time' in da.coords


class UpdateHistogram:
    def __init__(
        self,
        data_key: ResultKey,
        coord_keys: list[ResultKey],
        params: pydantic.BaseModel,
        job_number: JobNumber,
        data_service: DataService[ResultKey, sc.DataArray],
    ) -> None:
        self._data_key = data_key
        self._data_service = data_service
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
        if len(self._coords) == 1:
            x_name = next(iter(self._coords.values()))
            self._histogrammer = CorrelationHistogrammer(edges={x_name: params.x_edges})
        elif len(self._coords) == 2:
            x_name, y_name = list(self._coords.values())
            self._histogrammer = CorrelationHistogrammer(
                edges={x_name: params.x_edges, y_name: params.y_edges}
            )
        else:
            raise ValueError("Only 1D and 2D correlation histograms are supported.")

    def send(self, data: dict[ResultKey, sc.DataArray]) -> None:
        coords = {name: data[key] for key, name in self._coords.items()}
        result = self._histogrammer(data[self._data_key], coords=coords)
        # TODO Ensure still in transaction, or fix DataService
        self._data_service[self._result_key] = result


class CorrelationHistogrammerBuilder:
    def __init__(
        self,
        data_key: ResultKey,
        coord_keys: list[ResultKey],
        params: pydantic.BaseModel,
        job_number: JobNumber,
        data_service: DataService[ResultKey, sc.DataArray],
    ) -> None:
        self._data_key = data_key
        self._coord_keys = coord_keys
        self._params = params
        self._job_number = job_number
        self._data_service = data_service

    def create_pipe(self, data: dict[ResultKey, sc.DataArray]) -> UpdateHistogram:
        update = UpdateHistogram(
            data_key=self._data_key,
            coord_keys=self._coord_keys,
            params=self._params,
            job_number=self._job_number,
            data_service=self._data_service,
        )
        update.send(data)
        return update


class CorrelationHistogrammer:
    def __init__(self, edges: dict[str, EdgesWithUnit]) -> None:
        self._edges = {
            name: make_edges(model=model, dim=name, unit=model.unit)
            for name, model in edges.items()
        }

    def __call__(
        self, data: sc.DataArray, coords: dict[str, sc.DataArray]
    ) -> sc.DataArray:
        dependent = data.copy(deep=False)
        for dim in self._edges:
            lut = sc.lookup(sc.values(coords[dim]), mode='previous')
            dependent.coords[dim] = lut[dependent.coords['time']]
        return dependent.hist(**self._edges)
