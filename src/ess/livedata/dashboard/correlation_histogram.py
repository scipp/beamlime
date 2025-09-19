# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any, Generic, TypeVar

import numpy as np
import pydantic
import scipp as sc

from ess.livedata.config.workflow_spec import (
    JobId,
    JobNumber,
    ResultKey,
    WorkflowId,
    WorkflowSpec,
)
from ess.livedata.parameter_models import EdgesModel, make_edges

from .data_service import DataService
from .stream_manager import StreamManager
from .widgets.configuration_widget import ConfigurationAdapter


class EdgesWithUnit(EdgesModel):
    # Frozen so it cannot be changed in the UI but allows us to display the unit. If we
    # wanted to auto-generate an enum to get a dropdown menu, we probably cannot write
    # generic code below, since we would need to create all the models on the fly.
    unit: str = pydantic.Field(..., description="Unit for the edges", frozen=True)

    def make_edges(self, dim: str) -> sc.Variable:
        return make_edges(model=self, dim=dim, unit=self.unit)


class CorrelationHistogramParams(pydantic.BaseModel):
    # For now this is empty, will likely add params for, e.g., start_time and
    # normalization in the future.
    pass


class CorrelationHistogram1dParams(CorrelationHistogramParams):
    x_edges: EdgesWithUnit


class CorrelationHistogram2dParams(CorrelationHistogramParams):
    x_edges: EdgesWithUnit
    y_edges: EdgesWithUnit


# Next: Pass WorkflowId from this into BoundCorrelationHistogramController so that
# it could launch job on backend?


def make_workflow_spec(
    source_names: list[str], aux_source_names: list[str]
) -> WorkflowSpec:
    ndim = len(aux_source_names)
    params = {1: CorrelationHistogram1dParams, 2: CorrelationHistogram2dParams}
    return WorkflowSpec(
        instrument='frontend',  # As long as we are running in the frontend
        namespace='correlation',
        name=f'correlation_histogram_{ndim}d',
        title=f'{ndim}D Correlation Histogram',
        version=1,
        description=f'{ndim}D correlation histogram workflow',
        source_names=source_names,
        aux_source_names=aux_source_names,
        params=params[ndim],
    )


def _make_edges_field(name: str, coord: sc.DataArray) -> Any:
    unit = str(coord.unit)
    low = coord.nanmin().value
    high = np.nextafter(coord.nanmax().value, np.inf)
    return pydantic.Field(
        default=EdgesWithUnit(start=low, stop=high, num_bins=50, unit=unit),
        title=f"{name} bins",
        description=f"Define the bin edges for histogramming in {name}.",
    )


Params = TypeVar('Params', bound=CorrelationHistogramParams)


class CorrelationHistogramConfigurationAdapter(ConfigurationAdapter[Params]):
    """
    Configuration adapter for correlation histogram.

    Used to auto-generate a widget for configuring the histogram using the generic
    :py:class:`ConfigurationWidget`.
    """

    def __init__(
        self,
        *,
        title: str,
        description: str,
        model_class: type[Params],
        source_names: list[str],
        initial_parameter_values: dict[str, pydantic.BaseModel] | None = None,
        start_action: Callable[[list[str], Params], None],
    ) -> None:
        self._title = title
        self._description = description
        self._model_class = model_class
        self._source_names = source_names
        self._initial_parameter_values = initial_parameter_values or {}
        self._start_action = start_action

    @property
    def title(self) -> str:
        return self._title

    @property
    def description(self) -> str:
        return self._description

    @property
    def model_class(self) -> type[Params]:
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
        self, selected_sources: list[str], parameter_values: Params
    ) -> bool:
        self._start_action(selected_sources=selected_sources, params=parameter_values)
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
        """
        Called by widget to get configuration for modal creation.

        Parameters
        ----------
        axis_keys:
            The keys of the timeseries in the DataService to use for mapping a timestamp
            of a dependent variable to a value of the coordinate variable. At this point
            it is not know which dependent timeseries the user will want to histogram,
            but the axis keys can be used to configure the bin edges for the (one or
            two) dimensions of the correlation histogram. The axis keys will also be
            forwarded to the workflow when started so that the correlation histogram
            can be computed.
        """
        # All timeseries except the axis keys can be used as dependent variables. These
        # will thus be shown by the widget in the "source selection" menu.
        result_keys = [key for key in self.get_timeseries() if key not in axis_keys]
        source_names = [key.job_id.source_name for key in result_keys]
        coords = {key.job_id.source_name: self._data_service[key] for key in axis_keys}
        ndim = len(coords)
        fields = [_make_edges_field(name, coord) for name, coord in coords.items()]
        match ndim:
            case 1:

                class Configured1dParams(CorrelationHistogram1dParams):
                    x_edges: EdgesWithUnit = fields[0]

                bound_controller = BoundCorrelationHistogramController[
                    CorrelationHistogram1dParams
                ](data_keys=result_keys, axis_keys=axis_keys, controller=self)
                return CorrelationHistogramConfigurationAdapter[
                    CorrelationHistogram1dParams
                ](
                    title=f"{ndim}D Correlation Histogram",
                    description=f"Configure parameters for a {ndim}D correlation histogram.",
                    model_class=Configured1dParams,
                    source_names=source_names,
                    start_action=bound_controller.start_workflows,
                )
            case 2:

                class Configured2dParams(CorrelationHistogram2dParams):
                    x_edges: EdgesWithUnit = fields[0]
                    y_edges: EdgesWithUnit = fields[1]

                bound_controller = BoundCorrelationHistogramController[
                    CorrelationHistogram2dParams
                ](data_keys=result_keys, axis_keys=axis_keys, controller=self)
                return CorrelationHistogramConfigurationAdapter[
                    CorrelationHistogram2dParams
                ](
                    title=f"{ndim}D Correlation Histogram",
                    description=f"Configure parameters for a {ndim}D correlation histogram.",
                    model_class=Configured2dParams,
                    source_names=source_names,
                    start_action=bound_controller.start_workflows,
                )
            case _:
                raise ValueError(
                    "Expected 1 or 2 coordinates for correlation histogram."
                )


class BoundCorrelationHistogramController(Generic[Params]):
    def __init__(
        self,
        data_keys: list[ResultKey],
        axis_keys: list[ResultKey],
        controller: CorrelationHistogramController,
    ) -> None:
        self._data_keys = data_keys
        self._axis_keys = axis_keys
        self._controller = controller

    def start_workflows(self, selected_sources: list[str], params: Params) -> None:
        """
        Called by widget when user starts the workflow with concrete params.

        Note: Currently the "correlation" jobs run in the frontend process, essentially
        as a postprocessing step when new data arrives. There are considerations around
        moving this into a separate backend service, after the primary services.

        Parameters
        ----------
        data_keys:
            The data keys to run the correlation histogram on. One result per key will
            be generated.
        axis_keys:
            The axis keys to use for all of the correlation histograms.
        params:
            The parameters to use for the correlation histograms.
        """
        data_keys = [
            key for key in self._data_keys if key.job_id.source_name in selected_sources
        ]
        data = {key: self._controller._data_service[key] for key in data_keys}
        axes = {key: self._controller._data_service[key] for key in self._axis_keys}
        job_number = uuid.uuid4()  # New unique job number shared by all workflows

        # Note: If we switch to running correlation histogramming in a separate
        # service, at this point we would create a WorkflowConfig (for each data key),
        # with the axis keys as auxiliary source names and submit it. What follows is a
        # local setup and run of the correlation histogrammer workflow.

        if isinstance(params, CorrelationHistogram1dParams):
            edges = [params.x_edges]
        elif isinstance(params, CorrelationHistogram2dParams):
            edges = [params.x_edges, params.y_edges]
        else:
            raise ValueError("Expected 1d or 2d correlation histogram parameters.")
        for key, value in data.items():
            pipe_factory = make_correlation_histogrammer_pipe_factory(
                data_key=key,
                coord_keys=self._axis_keys,
                edges_params=edges,
                job_number=job_number,
                data_service=self._controller._data_service,
            )
            stream_manager = StreamManager(
                data_service=self._controller._data_service, pipe_factory=pipe_factory
            )
            # Subscribes to DataService internally
            pipe = stream_manager.make_merging_stream({key: value, **axes})
            # Keep a reference to avoid garbage collection
            self._controller._pipes.append(pipe)


def _is_timeseries(da: sc.DataArray) -> bool:
    return da.dims == ('time',) and 'time' in da.coords


def make_correlation_histogrammer_pipe_factory(
    data_key: ResultKey,
    coord_keys: list[ResultKey],
    edges_params: list[EdgesWithUnit],
    job_number: JobNumber,
    data_service: DataService[ResultKey, sc.DataArray],
) -> Any:
    class CorrelationHistogrammerPipe:
        """
        Connector of Pipe expected by StreamManager to CorrelationHistogrammer.

        When data is sent to the pipe, it runs the histogrammer and writes the result
        back to the DataService.
        """

        def __init__(self, data: dict[ResultKey, sc.DataArray]) -> None:
            self._data_key = data_key
            self._data_service = data_service
            self._coords = {key: key.job_id.source_name for key in coord_keys}
            edges = {
                dim: edge.make_edges(dim=dim)
                for dim, edge in zip(self._coords.values(), edges_params, strict=True)
            }
            self._histogrammer = CorrelationHistogrammer(edges=edges)
            self._result_key = ResultKey(
                workflow_id=WorkflowId(
                    instrument=data_key.workflow_id.instrument,
                    namespace='correlation',
                    # Name includes coords to avoid collisions. This won't work if the
                    # backend wants to translate from a WorkflowId to a WorkflowSpec.
                    # Ultimately we want the WorkflowSpec.aux_source_names to carry this
                    # information, instead of having workflows created dynamically.
                    # As the backend is job-based and we have a UUID for the job number,
                    # this should not be a problem.
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
