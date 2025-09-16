# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

from enum import Enum
from typing import Any

import numpy as np
import pydantic
import scipp as sc

from ess.livedata.config.workflow_spec import JobId, JobNumber, ResultKey, WorkflowId
from ess.livedata.parameter_models import EdgesModel

from .data_service import DataService
from .stream_manager import StreamManager
from .widgets.configuration_widget import ConfigurationAdapter


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
        initial_parameter_values: dict[str, pydantic.BaseModel] | None = None,
        data_service: DataService,
    ) -> None:
        self._title = title
        self._description = description
        self._model_class = model_class
        self._source_names = {
            result_key.job_id.source_name: result_key for result_key in result_keys
        }
        self._initial_parameter_values = initial_parameter_values or {}
        self._data_service = data_service

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

        # dict[ResultKey, sc.DataArray]
        pipe = self._stream_manager.make_merging_stream(items)

    def get_timeseries(self) -> list[ResultKey]:
        return [key for key, da in self._data_service.items() if _is_timeseries(da)]

    def create_1d(self, x_key: ResultKey) -> CorrelationHistogramConfigurationAdapter:
        x_data = self._data_service[x_key]
        x_name = x_key.job_id.source_name
        result_keys = [key for key in self.get_timeseries() if key != x_key]
        return CorrelationHistogramConfigurationAdapter(
            title="1D Correlation Histogram",
            description="Configure parameters for a 1D correlation histogram.",
            model_class=make_params_1d(x_name=x_name, x_data=x_data),
            result_keys=result_keys,
            data_service=self._data_service,
        )


def _is_timeseries(da: sc.DataArray) -> bool:
    return da.dims == ('time',) and 'time' in da.coords
