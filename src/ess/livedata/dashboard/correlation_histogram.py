# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2025 Scipp contributors (https://github.com/scipp)
from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import numpy as np
import pydantic
import scipp as sc

from ess.livedata.config.workflow_spec import JobId, JobNumber, ResultKey, WorkflowSpec
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


# Note: make_workflow_spec encapsulates workflow configuration in the widely-used
# WorkflowSpec format. In the future, this can be converted to WorkflowConfig and
# submitted to a separate backend service for execution.
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


class CorrelationHistogramConfigurationAdapter(
    ConfigurationAdapter[CorrelationHistogramParams]
):
    """
    Combined configuration adapter and workflow controller for correlation histograms.

    This class both generates the UI configuration for correlation histograms and
    handles workflow execution. It implements the ConfigurationAdapter interface
    for UI generation while also managing the actual computation workflow.
    """

    def __init__(
        self,
        data_keys: list[ResultKey],
        axis_keys: list[ResultKey],
        controller: CorrelationHistogramController,
    ) -> None:
        self._data_keys = data_keys
        self._axis_keys = axis_keys
        self._controller = controller

        # Create workflow spec to derive configuration
        aux_source_names = [key.job_id.source_name for key in axis_keys]
        source_names = [key.job_id.source_name for key in data_keys]
        self._workflow_spec = make_workflow_spec(source_names, aux_source_names)

        # Generate dynamic parameter model with bin edges
        coords = {
            key.job_id.source_name: controller._data_service[key] for key in axis_keys
        }
        self._model_class = self._create_dynamic_model_class(coords)

    def _create_dynamic_model_class(
        self, coords: dict[str, sc.DataArray]
    ) -> type[CorrelationHistogramParams]:
        """Create dynamic parameter model class with appropriate bin edge fields."""
        ndim = len(coords)
        fields = [_make_edges_field(dim, coord) for dim, coord in coords.items()]

        if ndim == 1:

            class Configured1dParams(CorrelationHistogram1dParams):
                x_edges: EdgesWithUnit = fields[0]

            return Configured1dParams
        if ndim == 2:

            class Configured2dParams(CorrelationHistogram2dParams):
                x_edges: EdgesWithUnit = fields[0]
                y_edges: EdgesWithUnit = fields[1]

            return Configured2dParams

        raise ValueError("Expected 1 or 2 coordinates for correlation histogram.")

    @property
    def title(self) -> str:
        return self._workflow_spec.title

    @property
    def description(self) -> str:
        return self._workflow_spec.description

    @property
    def model_class(self) -> type[CorrelationHistogramParams]:
        return self._model_class

    @property
    def source_names(self) -> list[str]:
        return self._workflow_spec.source_names

    @property
    def initial_source_names(self) -> list[str]:
        # In practice we may have many timeseries. We do not want to auto-populate the
        # selection since typically the user will want to select just one or a few.
        return []

    @property
    def initial_parameter_values(self) -> dict[str, Any]:
        return {}

    def start_action(
        self, selected_sources: list[str], parameter_values: CorrelationHistogramParams
    ) -> bool:
        """
        Execute the correlation histogram workflow with the given parameters.

        Note: Currently the "correlation" jobs run in the frontend process, essentially
        as a postprocessing step when new data arrives. There are considerations around
        moving this into a separate backend service, after the primary services, where
        the WorkflowSpec could be converted to WorkflowConfig for submission.
        """
        self._start_workflows(
            selected_sources=selected_sources, params=parameter_values
        )
        return True

    def _start_workflows(
        self, selected_sources: list[str], params: CorrelationHistogramParams
    ) -> None:
        """Internal workflow execution logic."""
        data_keys = [
            key for key in self._data_keys if key.job_id.source_name in selected_sources
        ]
        data = {key: self._controller._data_service[key] for key in data_keys}
        axes = {key: self._controller._data_service[key] for key in self._axis_keys}
        job_number = uuid.uuid4()  # New unique job number shared by all workflows

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
                result_callback=self._create_result_callback(key, job_number),
            )
            stream_manager = StreamManager(
                data_service=self._controller._data_service, pipe_factory=pipe_factory
            )
            # Subscribes to DataService internally
            pipe = stream_manager.make_merging_stream({key: value, **axes})
            # Keep a reference to avoid garbage collection
            self._controller._pipes.append(pipe)

    def _create_result_callback(
        self, data_key: ResultKey, job_number: JobNumber
    ) -> Callable[[sc.DataArray], None]:
        """Create callback for handling histogram results."""
        result_key = ResultKey(
            workflow_id=self._workflow_spec.get_id(),
            job_id=JobId(
                source_name=data_key.job_id.source_name, job_number=job_number
            ),
            output_name=None,
        )

        def callback(result: sc.DataArray) -> None:
            self._controller._data_service[result_key] = result

        return callback


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

        return CorrelationHistogramConfigurationAdapter(
            data_keys=result_keys, axis_keys=axis_keys, controller=self
        )


def _is_timeseries(da: sc.DataArray) -> bool:
    return da.dims == ('time',) and 'time' in da.coords


def make_correlation_histogrammer_pipe_factory(
    data_key: ResultKey,
    coord_keys: list[ResultKey],
    edges_params: list[EdgesWithUnit],
    result_callback: Callable[[sc.DataArray], None],
) -> Any:
    class CorrelationHistogrammerPipe:
        """
        Connector of Pipe expected by StreamManager to CorrelationHistogrammer.

        When data is sent to the pipe, it runs the histogrammer and sends the result
        via the provided callback.
        """

        def __init__(self, data: dict[ResultKey, sc.DataArray]) -> None:
            self._data_key = data_key
            self._result_callback = result_callback
            self._coords = {key: key.job_id.source_name for key in coord_keys}
            edges = {
                dim: edge.make_edges(dim=dim)
                for dim, edge in zip(self._coords.values(), edges_params, strict=True)
            }
            self._histogrammer = CorrelationHistogrammer(edges=edges)
            self.send(data)

        def send(self, data: dict[ResultKey, sc.DataArray]) -> None:
            coords = {name: data[key] for key, name in self._coords.items()}
            result = self._histogrammer(data[self._data_key], coords=coords)
            self._result_callback(result)

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
