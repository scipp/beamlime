# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import logging
import threading
import time
from contextlib import ExitStack

import plotly.graph_objects as go
import scipp as sc
from dash import dcc, html
from dash.exceptions import PreventUpdate

from beamlime import Service, ServiceBase
from beamlime.config import config_names, models
from beamlime.config.config_loader import load_config
from beamlime.config.instruments import get_config
from beamlime.config.models import ConfigKey, WorkflowConfig
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.config_service import ConfigService
from beamlime.core.message import StreamKind, compact_messages
from beamlime.dashboard_dash.dash_app import make_dash_app
from beamlime.dashboard_dash.parameter_widget import create_parameter_widget
from beamlime.dashboard_dash.plots import create_detector_plot
from beamlime.dashboard_dash.workflow_widget import create_workflow_controls
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.message_adapter import (
    AdaptingMessageSource,
    ChainedAdapter,
    Da00ToScippAdapter,
    KafkaToDa00Adapter,
)
from beamlime.kafka.source import KafkaMessageSource


class DashboardApp(ServiceBase):
    def __init__(
        self,
        *,
        instrument: str = 'dummy',
        dev: bool,
        debug: bool = False,
        log_level: int = logging.INFO,
        auto_remove_plots_after_seconds: float = 10.0,
    ) -> None:
        name = f'{instrument}_dashboard'
        super().__init__(name=name, log_level=log_level)

        self._instrument = instrument
        self._debug = debug

        # Initialize state
        self._plots: dict[str, go.Figure] = {}
        self._auto_remove_plots_after_seconds = auto_remove_plots_after_seconds
        self._last_plot_update: dict[str, float] = {}

        self._exit_stack = ExitStack()
        self._exit_stack.__enter__()

        # Load instrument configuration for source names
        self._instrument_config = get_config(instrument)

        # Setup services
        self._setup_config_service()
        self._source = self._setup_kafka_consumer()

        # Initialize Dash
        self._app = make_dash_app(name=name, callbacks=self)

    @property
    def server(self):
        """Return the Flask server for gunicorn"""
        return self._app.server

    def _setup_config_service(self) -> None:
        kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
        _, consumer = self._exit_stack.enter_context(
            kafka_consumer.make_control_consumer(instrument=self._instrument)
        )
        self._config_service = ConfigService(
            kafka_config={**kafka_downstream_config},
            consumer=consumer,
            topic=stream_kind_to_topic(
                instrument=self._instrument, kind=StreamKind.BEAMLIME_CONFIG
            ),
            logger=self._logger,
        )
        self._config_service_thread = threading.Thread(
            target=self._config_service.start
        )

    def _get_available_workflows(self) -> list[tuple[str, str, list[str], list[dict]]]:
        """Get available workflows from the config service."""
        config_key = ConfigKey(service_name="data_reduction", key="workflow_specs")
        workflow_specs = self._config_service.get(config_key)
        if workflow_specs is None:
            return []
        return [
            (hash, spec['name'], spec['source_names'], spec.get('parameters', []))
            for hash, spec in workflow_specs['workflows'].items()
        ]

    def _get_all_source_names(self) -> list[str]:
        """Get all source names from workflow specs and instrument config."""
        workflow_source_names = set()
        for _, _, source_names, _ in self._get_available_workflows():
            workflow_source_names.update(source_names)

        # Combine with instrument config source names
        all_source_names = list(workflow_source_names)
        all_source_names.sort()  # Sort for consistent display
        return all_source_names

    def _get_workflow_options_for_source(self, source_name: str) -> list[dict]:
        """Get workflow options that are compatible with the given source name."""
        available_workflows = self._get_available_workflows()
        compatible_workflows = []

        for hash_id, name, source_names, _ in available_workflows:
            if source_name in source_names:
                compatible_workflows.append({'label': name, 'value': hash_id})

        return compatible_workflows

    def _get_workflow_parameters(self, workflow_id: str) -> list[dict]:
        """Get parameters for a specific workflow."""
        available_workflows = self._get_available_workflows()
        for hash_id, _, _, parameters in available_workflows:
            if hash_id == workflow_id:
                return parameters
        return []

    def _setup_kafka_consumer(self) -> AdaptingMessageSource:
        consumer_config = load_config(
            namespace=config_names.reduced_data_consumer, env=''
        )
        kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
        consumer = self._exit_stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=[
                    stream_kind_to_topic(
                        instrument=self._instrument, kind=StreamKind.BEAMLIME_DATA
                    )
                ],
                config={**consumer_config, **kafka_downstream_config},
                group='dashboard',
            )
        )
        return AdaptingMessageSource(
            source=KafkaMessageSource(consumer=consumer, num_messages=1000),
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(stream_kind=StreamKind.BEAMLIME_DATA),
                second=Da00ToScippAdapter(),
            ),
        )

    def _toggle_slider(self, checkbox_value):
        return len(checkbox_value) == 0

    def update_roi(self, x_center, x_delta, y_center, y_delta):
        x_min = max(0, x_center - x_delta)
        x_max = min(100, x_center + x_delta)
        y_min = max(0, y_center - y_delta)
        y_max = min(100, y_center + y_delta)

        roi = models.ROIRectangle(
            x=models.ROIAxisRange(low=x_min / 100, high=x_max / 100),
            y=models.ROIAxisRange(low=y_min / 100, high=y_max / 100),
        )
        config_key = ConfigKey(service_name="detector_data", key="roi_rectangle")
        self._config_service.update_config(config_key, roi.model_dump())

        # Update ROI rectangles in all 2D plots
        for fig in self._plots.values():
            if hasattr(fig.data[0], 'z'):  # Check if it's a 2D plot
                x_range = [fig.data[0].x[0], fig.data[0].x[-1]]
                y_range = [fig.data[0].y[0], fig.data[0].y[-1]]
                x_min_plot = x_range[0] + (x_range[1] - x_range[0]) * x_min / 100
                x_max_plot = x_range[0] + (x_range[1] - x_range[0]) * x_max / 100
                y_min_plot = y_range[0] + (y_range[1] - y_range[0]) * y_min / 100
                y_max_plot = y_range[0] + (y_range[1] - y_range[0]) * y_max / 100

                fig.update_shapes(
                    {
                        'x0': x_min_plot,
                        'x1': x_max_plot,
                        'y0': y_min_plot,
                        'y1': y_max_plot,
                        'visible': True,
                    }
                )

        return x_center, x_delta, y_center, y_delta

    def update_toa_range(self, center, delta, toa_enabled):
        low = center - delta / 2
        high = center + delta / 2
        model = models.TOARange(
            enabled=len(toa_enabled) > 0, low=low, high=high, unit='us'
        )
        config_key = ConfigKey(service_name="detector_data", key="toa_range")
        self._config_service.update_config(config_key, model.model_dump())
        return center, delta

    def update_use_weights(self, value: list[str]) -> list[str]:
        model = models.PixelWeighting(enabled=len(value) > 0)
        config_key = ConfigKey(service_name="detector_data", key="pixel_weighting")
        self._config_service.update_config(config_key, model.model_dump())
        return value

    def update_plots(self, n: int | None):
        if n is None:
            raise PreventUpdate

        now = time.time()
        try:
            messages = self._source.get_messages()
            num = len(messages)
            messages = compact_messages(messages)
            self._logger.info(
                "Got %d messages, showing most recent %d", num, len(messages)
            )
            for msg in messages:
                orig_source_name, service_name, suffix = msg.stream.name.split(
                    '/', maxsplit=2
                )
                key = f'Source name: {orig_source_name}<br>{suffix}'
                data = msg.value
                for dim in data.dims:
                    if dim not in data.coords:
                        data.coords[dim] = sc.arange(dim, data.sizes[dim], unit=None)
                if key not in self._plots:
                    self._plots[key] = create_detector_plot(key, data)
                fig = self._plots[key]
                if len(data.dims) == 1:
                    fig.data[0].x = data.coords[data.dim].values
                    fig.data[0].y = data.values
                else:  # 2D
                    y_dim, x_dim = data.dims
                    fig.data[0].x = data.coords[x_dim].values
                    fig.data[0].y = data.coords[y_dim].values
                    fig.data[0].z = data.values
                self._last_plot_update[key] = now

        except Exception as e:
            self._logger.exception("Error in update_plots: %s", e)
            raise PreventUpdate from None

        # Remove plots if no recent update. This happens, e.g., when the reduction
        # workflow is removed or changed.
        for key, last_update in self._last_plot_update.items():
            if now - last_update > self._auto_remove_plots_after_seconds:
                self._plots.pop(key, None)
                self._logger.info("Removed plot for %s", key)

        graphs = [dcc.Graph(figure=fig) for fig in self._plots.values()]
        return [html.Div(graphs, style={'display': 'flex', 'flexWrap': 'wrap'})]

    def update_timing_settings(self, update_speed: float) -> float:
        update_every = models.UpdateEvery(value=2**update_speed, unit='ms')
        config_key = ConfigKey(key="update_every")
        self._config_service.update_config(config_key, update_every.model_dump())
        return 2**update_speed

    def update_num_points(self, value: int) -> int:
        config_key = ConfigKey(key="time_of_arrival_bins")
        self._config_service.update_config(config_key, value)
        return value

    def clear_data(self, n_clicks: int | None) -> int:
        if n_clicks is None or n_clicks == 0:
            raise PreventUpdate
        model = models.StartTime(value=int(time.time_ns()), unit='ns')
        config_key = ConfigKey(key="start_time")
        self._config_service.update_config(config_key, model.model_dump())
        return 0

    def stop_workflow(self, n_clicks: int | None, source_name: str | None) -> int:
        """Stop/disable a workflow for the selected source."""
        if n_clicks is None or n_clicks == 0 or not source_name:
            raise PreventUpdate

        config_key = ConfigKey(
            source_name=source_name,
            service_name="data_reduction",
            key="workflow_config",
        )
        self._config_service.update_config(config_key, None)
        return 0

    def send_workflow_control(
        self,
        n_clicks: int | None,
        source_name: str | None,
        workflow_id: str | None,
        param_values: list,
        param_types: list,
        param_ids: list,
    ) -> int:
        """Apply the selected workflow with parameter values."""
        if n_clicks is None or n_clicks == 0 or not source_name or workflow_id is None:
            raise PreventUpdate

        try:
            # Process parameter values based on their types
            param_dict = {}
            for value, param_type, param_id in zip(
                param_values, param_types, param_ids, strict=True
            ):
                # Extract parameter name from the id dictionary
                param_name = param_id.get('name', '')
                if not param_name:
                    continue

                # Convert value based on parameter type
                if param_type == 'BOOL':
                    # Convert checklist value to boolean
                    processed_value = len(value) > 0 and 'true' in value
                elif param_type == 'INT':
                    processed_value = int(value) if value is not None else None
                elif param_type == 'FLOAT':
                    processed_value = float(value) if value is not None else None
                else:
                    # String or other types
                    processed_value = value

                param_dict[param_name] = processed_value

            # Create WorkflowConfig
            workflow_config = WorkflowConfig(identifier=workflow_id, values=param_dict)

            # Send workflow config
            config_key = ConfigKey(
                source_name=source_name,
                service_name="data_reduction",
                key="workflow_config",
            )

            self._config_service.update_config(config_key, workflow_config.model_dump())
            self._logger.info(
                "Applied workflow %s to source %s with parameters: %s",
                workflow_id,
                source_name,
                param_dict,
            )

        except Exception as exc:
            self._logger.exception("Error applying workflow config: %s", exc)

        return 0

    def show_workflow_config(
        self, n_clicks: int | None, source_name: str | None
    ) -> list:
        """Show the workflow config panel when the Configure button is clicked."""
        if n_clicks is None or n_clicks == 0 or not source_name:
            return []

        try:
            workflow_options = self._get_workflow_options_for_source(source_name)

            workflow_id = workflow_options[0]['value'] if workflow_options else None

            # Initialize with basic workflow selection UI
            return create_workflow_controls(
                source_name=source_name,
                workflow_options=workflow_options,
                workflow_id=workflow_id,
            )

        except Exception as e:
            self._logger.warning("Failed to update workflow controls: %s", e)
            return [
                html.Div(
                    f"Error loading workflow options for {source_name}: {e}",
                    style={'color': 'red', 'margin': '10px 0'},
                )
            ]

    def update_workflow_parameters(
        self, workflow_id: str, source_name: str | None
    ) -> list:
        """Update parameter widgets when a workflow is selected."""
        self._logger.info(
            "Updating workflow parameters for workflow ID: %s", workflow_id
        )
        if not workflow_id or not source_name:
            return []

        try:
            parameters = self._get_workflow_parameters(workflow_id)

            if not parameters:
                return [html.Div("No configurable parameters for this workflow")]

            # Create parameter header
            parameter_widgets = [
                html.H4(
                    "Workflow Parameters",
                    style={'marginTop': '15px', 'marginBottom': '10px'},
                )
            ]

            # Create widgets for each parameter
            for param in parameters:
                parameter_widgets.extend(create_parameter_widget(param))

            return parameter_widgets

        except Exception as e:
            self._logger.warning("Failed to create parameter widgets: %s", e)
            return [
                html.Div(
                    f"Error loading parameters: {e}",
                    style={'color': 'red', 'margin': '10px 0'},
                )
            ]

    def update_source_dropdown(self, _: int, current_value: str | None) -> list[dict]:
        """
        Update the source dropdown with all unique source names from workflows.

        Preserves the current selection by passing it through.
        """
        options = [
            {'label': name, 'value': name} for name in self._get_all_source_names()
        ]

        # Ensure current value is in options if it's not None
        if current_value and not any(opt['value'] == current_value for opt in options):
            options.append(
                {'label': f"{current_value} (not found)", 'value': current_value}
            )

        return options

    def set_initial_source(
        self, options: list[dict], current_value: str | None
    ) -> str | None:
        """Set initial value for source dropdown only if it's currently empty."""
        if current_value is None and options:
            return options[0]['value']
        return current_value  # Keep existing selection

    def _start_impl(self) -> None:
        self._config_service_thread.start()
        # Wait briefly to allow config service to fetch initial configs
        time.sleep(0.5)

    def run_forever(self) -> None:
        """Only for development purposes."""
        self._app.run(debug=self._debug)

    def _stop_impl(self) -> None:
        """Clean shutdown of all components."""
        self._config_service.stop()
        self._config_service_thread.join()
        self._source.close()
        self._exit_stack.__exit__(None, None, None)


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = Service.setup_arg_parser(description='Beamlime Dashboard')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    return parser


def main() -> None:
    parser = setup_arg_parser()
    app = DashboardApp(**vars(parser.parse_args()))
    app.start(blocking=True)


if __name__ == '__main__':
    main()
