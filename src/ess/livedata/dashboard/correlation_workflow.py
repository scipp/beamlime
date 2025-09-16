# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import abc
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


class Pipe:
    def __init__(self, data: Any) -> None:
        pass

    def send(self, data: Any) -> None:
        pass


class CorrelationHistogramController:
    def __init__(self, data_service: DataService) -> None:
        self._data_service = data_service
        self._stream_manager = StreamManager(
            data_service=data_service, pipe_factory=Pipe
        )

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

    def start_workflows_1d(
        self,
        data_keys: list[ResultKey],
        axis_keys: list[ResultKey],
        params: pydantic.BaseModel,
    ) -> None:
        # TODO JobStatus reporting?? How to stop?
        data = {key: self._data_service[key] for key in data_keys}
        axes = {key: self._data_service[key] for key in axis_keys}
        x_name = next(iter(axes)).job_id.source_name

        # Feed back into data service, or plotting preprocessor?
        #
        histogrammer = CorrelationHistogrammer(edges={x_name: params.x_edges})
        for key, value in data.items():
            pipe = self._stream_manager.make_merging_stream({key: value, **axes})


def _is_timeseries(da: sc.DataArray) -> bool:
    return da.dims == ('time',) and 'time' in da.coords


class CorrelationHistogrammer:
    def __init__(self, edges: dict[str, EdgesWithUnit]) -> None:
        self._edges = {
            name: make_edges(model=model, dim=name, unit=model.unit)
            for name, model in edges.items()
        }

    def __call__(
        self, data: sc.DataArray, coords: dict[str, sc.DataArray]
    ) -> sc.DataArray:
        dependent = data.copy()
        for dim in self._edges:
            lut = sc.lookup(coords[dim], mode='previous')
            dependent.coords[dim] = lut[dependent.coords['time']]
        return dependent.hist(**self._edges)
