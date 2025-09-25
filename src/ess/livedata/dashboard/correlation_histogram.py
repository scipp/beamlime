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
from .data_subscriber import DataSubscriber, MergingStreamAssembler
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
        ndim: int,
        controller: CorrelationHistogramController,
    ) -> None:
        self._ndim = ndim
        self._controller = controller
        self._selected_aux_sources: dict[str, str] | None = None
        self._current_model_class: type[CorrelationHistogramParams] | None = None
        self._source_name_to_key: dict[str, ResultKey] = {}

        # Create workflow spec template
        self._workflow_spec_template = make_workflow_spec([], [])
        self._workflow_spec_template.name = f'correlation_histogram_{ndim}d'
        self._workflow_spec_template.title = f'{ndim}D Correlation Histogram'

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
        return self._workflow_spec_template.title

    @property
    def description(self) -> str:
        return self._workflow_spec_template.description

    @property
    def aux_source_names(self) -> dict[str, list[str]]:
        all_timeseries = self._controller.get_timeseries()
        # Build mapping from source name to key
        self._source_name_to_key = {
            key.job_id.source_name: key for key in all_timeseries
        }
        source_names = list(self._source_name_to_key.keys())

        if self._ndim == 1:
            return {'x_param': source_names}
        elif self._ndim == 2:
            return {'x_param': source_names, 'y_param': source_names}
        else:
            raise ValueError(f"Unsupported dimensionality: {self._ndim}")

    def model_class(
        self, aux_source_names: dict[str, str]
    ) -> type[CorrelationHistogramParams] | None:
        if not aux_source_names or len(aux_source_names) != self._ndim:
            self._selected_aux_sources = None
            self._current_model_class = None
            return None

        # Store selected aux sources for use in start_action
        self._selected_aux_sources = aux_source_names

        # Get coordinate data for selected aux sources using the cached mapping
        coords = {}
        for source_name in aux_source_names.values():
            key = self._source_name_to_key.get(source_name)
            if key is None:
                self._selected_aux_sources = None
                self._current_model_class = None
                return None
            coords[source_name] = self._controller.get_data(key)

        self._current_model_class = self._create_dynamic_model_class(coords)
        return self._current_model_class

    @property
    def source_names(self) -> list[str]:
        all_timeseries = self._controller.get_timeseries()
        return [key.job_id.source_name for key in all_timeseries]

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
        if self._selected_aux_sources is None:
            return False

        data_keys = [
            self._source_name_to_key[source_name] for source_name in selected_sources
        ]
        axis_keys = [
            self._source_name_to_key[source_name]
            for source_name in self._selected_aux_sources.values()
        ]

        data = {key: self._controller.get_data(key) for key in data_keys}
        axes = {key: self._controller.get_data(key) for key in axis_keys}
        job_number = uuid.uuid4()  # New unique job number shared by all workflows

        if isinstance(parameter_values, CorrelationHistogram1dParams):
            edges = [parameter_values.x_edges]
        elif isinstance(parameter_values, CorrelationHistogram2dParams):
            edges = [parameter_values.x_edges, parameter_values.y_edges]
        else:
            # Raising instead of returning False, since this should not happen if the
            # implementation is correct.
            raise ValueError("Expected 1d or 2d correlation histogram parameters.")

        for key, value in data.items():
            processor = CorrelationHistogramProcessor(
                data_key=key,
                coord_keys=axis_keys,
                edges_params=edges,
                result_callback=self._create_result_callback(key, job_number),
            )
            self._controller.add_correlation_processor(processor, {key: value, **axes})
        return True

    def _create_result_callback(
        self, data_key: ResultKey, job_number: JobNumber
    ) -> Callable[[sc.DataArray], None]:
        """Create callback for handling histogram results."""
        result_key = ResultKey(
            workflow_id=self._workflow_spec_template.get_id(),
            job_id=JobId(
                source_name=data_key.job_id.source_name, job_number=job_number
            ),
            output_name=None,
        )

        def callback(result: sc.DataArray) -> None:
            self._controller.set_data(result_key, result)

        return callback


class CorrelationHistogramController:
    def __init__(self, data_service: DataService[ResultKey, sc.DataArray]) -> None:
        self._data_service = data_service
        self._update_subscribers: list[Callable[[], None]] = []
        self._data_service.subscribe_to_changed_keys(self._on_data_keys_updated)
        self._processors: list[CorrelationHistogramProcessor] = []

    def get_data(self, key: ResultKey) -> sc.DataArray:
        """Get data for a given key."""
        return self._data_service[key]

    def set_data(self, key: ResultKey, data: sc.DataArray) -> None:
        """Set data for a given key."""
        self._data_service[key] = data

    def add_correlation_processor(
        self,
        processor: CorrelationHistogramProcessor,
        items: dict[ResultKey, sc.DataArray],
    ) -> None:
        """Add a correlation histogram processor with DataService subscription."""
        self._processors.append(processor)

        # Create subscriber that merges data and sends to processor
        assembler = MergingStreamAssembler(set(items))
        subscriber = DataSubscriber(assembler, processor)
        self._data_service.register_subscriber(subscriber)

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

    def create_1d_config(self) -> CorrelationHistogramConfigurationAdapter:
        """Create configuration adapter for 1D correlation histograms."""
        return CorrelationHistogramConfigurationAdapter(ndim=1, controller=self)

    def create_2d_config(self) -> CorrelationHistogramConfigurationAdapter:
        """Create configuration adapter for 2D correlation histograms."""
        return CorrelationHistogramConfigurationAdapter(ndim=2, controller=self)

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


class CorrelationHistogramProcessor:
    """Processes correlation histogram updates when data changes."""

    def __init__(
        self,
        data_key: ResultKey,
        coord_keys: list[ResultKey],
        edges_params: list[EdgesWithUnit],
        result_callback: Callable[[sc.DataArray], None],
    ) -> None:
        self._data_key = data_key
        self._result_callback = result_callback
        self._coords = {key: key.job_id.source_name for key in coord_keys}
        edges = {
            dim: edge.make_edges(dim=dim)
            for dim, edge in zip(self._coords.values(), edges_params, strict=True)
        }
        self._histogrammer = CorrelationHistogrammer(edges=edges)

    def send(self, data: dict[ResultKey, sc.DataArray]) -> None:
        """Called when data is updated - processes the correlation histogram."""
        coords = {name: data[key] for key, name in self._coords.items()}
        result = self._histogrammer(data[self._data_key], coords=coords)
        self._result_callback(result)


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
