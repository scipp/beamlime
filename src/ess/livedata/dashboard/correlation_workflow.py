# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import abc
import uuid
from collections.abc import Callable
from enum import Enum
from typing import Any

import numpy as np
import pydantic
import scipp as sc

from ess.livedata.config.workflow_spec import JobId, JobNumber, ResultKey, WorkflowId
from ess.livedata.parameter_models import EdgesModel, make_edges

from .data_service import DataService
from .stream_manager import StreamManager
from .widgets.configuration_widget import ConfigurationAdapter


class EdgesWithUnit(EdgesModel, abc.ABC):
    @property
    @abc.abstractmethod
    def unit(self) -> str:
        """Unit for the edges."""


def make_params_1d(*, x_name: str, x_data: sc.DataArray) -> type[pydantic.BaseModel]:
    xunit = x_data.unit
    xlow = x_data.nanmin().value
    xhigh = np.nextafter(x_data.nanmax().value, np.inf)

    class XUnit(str, Enum):
        ONLY_OPTION = xunit

    class XEdgesModel(EdgesModel):
        unit: XUnit = pydantic.Field(
            default=XUnit.ONLY_OPTION, description=f"Unit for {x_name} edges"
        )

    class CorrelationHistogramParams(pydantic.BaseModel):
        # TODO Add start_time, normalize
        x_edges: XEdgesModel = pydantic.Field(
            default=XEdgesModel(start=xlow, stop=xhigh, num_bins=50),
            title=f"{x_name} Edges",
            description=f"Bin edges for the {x_name} axis",
        )

    return CorrelationHistogramParams


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

    def create_config_1d(
        self, x_key: ResultKey
    ) -> CorrelationHistogramConfigurationAdapter:
        x_data = self._data_service[x_key]
        x_name = x_key.job_id.source_name
        result_keys = [key for key in self.get_timeseries() if key != x_key]
        return CorrelationHistogramConfigurationAdapter(
            title="1D Correlation Histogram",
            description="Configure parameters for a 1D correlation histogram.",
            model_class=make_params_1d(x_name=x_name, x_data=x_data),
            result_keys=result_keys,
            axis_keys=[x_key],
            start_action=self.start_workflows_1d,
        )

    def create_config_2d(
        self, x_key: ResultKey, y_key: ResultKey
    ) -> CorrelationHistogramConfigurationAdapter:
        raise NotImplementedError("2D correlation histograms are not yet implemented.")

    def start_workflows_1d(
        self,
        data_keys: list[ResultKey],
        axis_keys: list[ResultKey],
        params: pydantic.BaseModel,
    ) -> None:
        # TODO JobStatus reporting?? How to stop?
        data = {key: self._data_service[key] for key in data_keys}
        axes = {key: self._data_service[key] for key in axis_keys}
        job_number = uuid.uuid4()  # New unique job number
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

        print(data_key.job_id.source_name)
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

    def send(self, data: Any) -> None:
        print(f'Updating {self._result_key}')
        coords = {name: data[key] for key, name in self._coords.items()}
        result = self._histogrammer(data[self._data_key], coords=coords)
        # This is bad since it will cause notifications to everything
        print(f'{self._result_key.job_id.source_name} -> {result.max().value}')
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

    def create_pipe(self, data: Any) -> UpdateHistogram:
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
