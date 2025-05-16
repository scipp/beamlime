# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import logging
import threading
import time
from contextlib import ExitStack

import plotly.graph_objects as go
import scipp as sc
from dash import ALL, Dash, Input, Output, State, dcc, html
from dash.exceptions import PreventUpdate

from beamlime import Service, ServiceBase
from beamlime.config import config_names, models
from beamlime.config.config_loader import load_config
from beamlime.config.instruments import get_config
from beamlime.config.models import ConfigKey, WorkflowConfig
from beamlime.config.streams import stream_kind_to_topic
from beamlime.core.config_service import ConfigService
from beamlime.core.message import StreamKind, compact_messages
from beamlime.dashboard.plots import create_detector_plot
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
        self._app = Dash(name)
        self._setup_layout()
        self._setup_callbacks()

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

    def _create_parameter_widget(self, param: dict) -> list:
        """Create appropriate widget based on parameter type."""
        param_type = param.get('param_type', 'STRING').upper()
        unit = param.get('unit')
        default_value = param.get('default', '')
        description = param.get('description', '')
        widget_id = {'type': 'param-input', 'name': param['name']}

        # Create label with tooltip for description
        label = html.Label(
            param['name'] if not unit else f'{param["name"]} [{unit}]',
            title=description,  # Tooltip on hover
            style={'cursor': 'help' if description else 'default'},
        )

        # Create appropriate input widget based on parameter type
        if param_type == 'BOOL':
            input_widget = dcc.Checklist(
                id=widget_id,
                options=[{'label': '', 'value': 'true'}],
                value=['true'] if default_value else [],
                style={'margin': '5px 0'},
            )
        elif param_type == 'INT':
            input_widget = dcc.Input(
                id=widget_id,
                type='number',
                step=1,
                value=default_value,
                style={'width': '100%'},
            )
        elif param_type == 'FLOAT':
            input_widget = dcc.Input(
                id=widget_id,
                type='number',
                step=0.1,
                value=default_value,
                style={'width': '100%'},
            )
        elif param_type == 'OPTIONS' and 'options' in param:
            options = [{'label': opt, 'value': opt} for opt in param['options']]
            input_widget = dcc.Dropdown(
                id=widget_id,
                options=options,
                value=default_value
                if default_value in param['options']
                else param['options'][0],
                style={'width': '100%'},
            )
        else:  # Default to string input for any other type
            input_widget = dcc.Input(
                id=widget_id,
                type='text',
                value=str(default_value),
                style={'width': '100%'},
            )

        # Add hidden div to store parameter type for value conversion
        param_type_store = html.Div(
            id={'type': 'param-type', 'name': param['name']},
            children=param_type,
            style={'display': 'none'},
        )

        return [
            label,
            input_widget,
            param_type_store,
            html.Div(style={'marginBottom': '10px'}),
        ]

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

    def _setup_layout(self) -> None:
        # Add CSS styles using the Dash assets approach
        self._app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            html, body {
                margin: 0;
                padding: 0;
                overflow-x: hidden;
                height: 100%;
                max-height: 100%;
            }
            #react-entry-point {
                height: 100%;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

        controls = [
            html.Label('Update Speed (ms)'),
            dcc.Slider(
                id='update-speed',
                min=8,
                max=13,
                step=0.5,
                value=10,
                marks={i: {'label': f'{2**i}'} for i in range(8, 14)},
            ),
            dcc.Checklist(
                id='bins-checkbox',
                options=[
                    {
                        'label': 'Time-of-arrival bins (WARNING: Clears the history!)',
                        'value': 'confirmed',
                    }
                ],
                value=[],
                style={'margin': '10px 0'},
            ),
            dcc.Slider(
                id='num-points',
                min=10,
                max=1000,
                step=10,
                value=100,
                marks={i: str(i) for i in range(0, 1001, 100)},
                disabled=True,
            ),
            html.Label('ROI X-axis Center (%)'),
            dcc.Slider(
                id='roi-x-center',
                min=0,
                max=100,
                step=1,
                value=50,
                marks={i: str(i) for i in range(0, 101, 20)},
            ),
            html.Label('ROI X-axis Width (%)'),
            dcc.Slider(
                id='roi-x-delta',
                min=0,
                max=10,
                step=1,
                value=5,
                marks={i: str(i) for i in range(0, 11, 1)},
            ),
            html.Label('ROI Y-axis Center (%)'),
            dcc.Slider(
                id='roi-y-center',
                min=0,
                max=100,
                step=1,
                value=50,
                marks={i: str(i) for i in range(0, 101, 20)},
            ),
            html.Label('ROI Y-axis Width (%)'),
            dcc.Slider(
                id='roi-y-delta',
                min=0,
                max=10,
                step=1,
                value=5,
                marks={i: str(i) for i in range(0, 11, 1)},
            ),
            dcc.Checklist(
                id='toa-checkbox',
                options=[
                    {'label': 'Filter by time-of-arrival (μs)', 'value': 'enabled'}
                ],
                value=[],
                style={'margin': '10px 0'},
            ),
            html.Label('Time-of-arrival center (μs)'),
            dcc.Slider(
                id='toa-center',
                min=0,
                max=71_000,
                step=100,
                value=35_500,
                marks={i: str(i) for i in range(0, 71_001, 10_000)},
            ),
            html.Label('Time-of-arrival width (μs)'),
            dcc.Slider(
                id='toa-delta',
                min=0,
                max=5_000,
                step=100,
                value=5_000,
                marks={i: str(i) for i in range(0, 5_001, 1000)},
            ),
            dcc.Checklist(
                id='use-weights-checkbox',
                options=[{'label': 'Use weights', 'value': 'enabled'}],
                value=['enabled'],
                style={'margin': '10px 0'},
            ),
            html.Button('Clear', id='clear-button', n_clicks=0),
            html.Hr(style={'margin': '20px 0'}),
            html.H3('Workflow Control', style={'marginTop': '10px'}),
            html.Label('Source Name'),
            dcc.Dropdown(
                id='workflow-source-name',
                options=[],  # Will be populated dynamically
                value=None,
                style={'width': '100%', 'marginBottom': '10px'},
            ),
            html.Div(
                style={
                    'display': 'flex',
                    'justifyContent': 'space-between',
                    'marginTop': '10px',
                },
                children=[
                    html.Button(
                        'Stop Workflow',
                        id='workflow-stop-button',
                        n_clicks=0,
                        style={'width': '48%'},
                    ),
                    html.Button(
                        'Configure Workflow',
                        id='workflow-configure-button',
                        n_clicks=0,
                        style={'width': '48%'},
                    ),
                ],
            ),
            html.Div(
                id='workflow-controls-container',
                children=[],  # Will be populated when Configure Workflow is clicked
                style={'marginTop': '10px'},
            ),
            html.Label('Note that workflow changes may take a few seconds to apply.'),
        ]
        self._app.layout = html.Div(
            [
                html.Div(
                    controls,
                    style={
                        'width': '300px',
                        'position': 'fixed',
                        'top': '0',
                        'left': '0',
                        'bottom': '0',
                        'padding': '10px',
                        'overflowY': 'auto',
                        'backgroundColor': '#f8f9fa',
                        'borderRight': '1px solid #dee2e6',
                        'zIndex': '1000',
                    },
                ),
                html.Div(
                    id='plots-container',
                    style={
                        'marginLeft': '320px',
                        'padding': '10px 10px 0 10px',  # Remove bottom padding
                        'height': '100vh',
                        'overflowY': 'auto',
                        'boxSizing': 'border-box',
                    },
                ),
                dcc.Interval(id='interval-component', interval=200, n_intervals=0),
                # Add interval for workflow updates
                dcc.Interval(
                    id='workflow-update-interval', interval=5000, n_intervals=0
                ),
            ],
            style={
                'height': '100vh',
                'width': '100%',
                'margin': '0',
                'padding': '0',
                'overflow': 'hidden',  # Hide both x and y overflow
                'boxSizing': 'border-box',
                'display': 'block',  # Ensure block display
            },
        )

    def _toggle_slider(self, checkbox_value):
        return len(checkbox_value) == 0

    def _setup_callbacks(self) -> None:
        self._app.callback(
            Output('num-points', 'disabled'), Input('bins-checkbox', 'value')
        )(self._toggle_slider)

        self._app.callback(
            Output('plots-container', 'children'),
            Input('interval-component', 'n_intervals'),
        )(self.update_plots)

        self._app.callback(
            Output('interval-component', 'interval'), Input('update-speed', 'value')
        )(self.update_timing_settings)

        self._app.callback(Output('num-points', 'value'), Input('num-points', 'value'))(
            self.update_num_points
        )

        self._app.callback(
            Output('clear-button', 'n_clicks'), Input('clear-button', 'n_clicks')
        )(self.clear_data)

        self._app.callback(
            Output('roi-x-center', 'value'),
            Output('roi-x-delta', 'value'),
            Output('roi-y-center', 'value'),
            Output('roi-y-delta', 'value'),
            Input('roi-x-center', 'value'),
            Input('roi-x-delta', 'value'),
            Input('roi-y-center', 'value'),
            Input('roi-y-delta', 'value'),
        )(self.update_roi)

        self._app.callback(
            [Output('toa-center', 'disabled'), Output('toa-delta', 'disabled')],
            Input('toa-checkbox', 'value'),
        )(lambda value: [len(value) == 0, len(value) == 0])

        self._app.callback(
            Output('toa-center', 'value'),
            Output('toa-delta', 'value'),
            Input('toa-center', 'value'),
            Input('toa-delta', 'value'),
            Input('toa-checkbox', 'value'),
        )(self.update_toa_range)

        self._app.callback(
            Output('use-weights-checkbox', 'value'),
            Input('use-weights-checkbox', 'value'),
        )(self.update_use_weights)

        self._app.callback(
            Output('workflow-controls-container', 'children'),
            Input('workflow-configure-button', 'n_clicks'),
            State('workflow-source-name', 'value'),
        )(self.show_workflow_config)
        self._app.callback(
            Output('workflow-params-container', 'children'),
            Input('workflow-name', 'value'),
            State('workflow-source-name', 'value'),
        )(self.update_workflow_parameters)
        self._app.callback(
            Output('workflow-stop-button', 'n_clicks'),
            Input('workflow-stop-button', 'n_clicks'),
            Input('workflow-source-name', 'value'),
        )(self.stop_workflow)

        # Update source name dropdown options periodically
        self._app.callback(
            Output('workflow-source-name', 'options'),
            Input('workflow-update-interval', 'n_intervals'),
            Input('workflow-source-name', 'value'),
        )(self.update_source_dropdown)

        # Set initial value for source dropdown ONLY if empty and only once
        self._app.callback(
            Output('workflow-source-name', 'value'),
            Input('workflow-source-name', 'options'),
            Input('workflow-source-name', 'value'),
            prevent_initial_call=True,  # Prevent automatic call on initial load
        )(self.set_initial_source)

        # Update workflow apply button callback to collect parameter values
        self._app.callback(
            Output('workflow-apply-button', 'n_clicks'),
            Input('workflow-apply-button', 'n_clicks'),
            State('workflow-source-storage', 'children'),
            State('workflow-name', 'value'),
            # Use ALL pattern to get all parameter inputs
            State({'type': 'param-input', 'name': ALL}, 'value'),
            State({'type': 'param-type', 'name': ALL}, 'children'),
            State({'type': 'param-input', 'name': ALL}, 'id'),
        )(self.send_workflow_control)

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
            controls = [
                html.Div(
                    f"Configuring workflows for source: {source_name}",
                    style={'fontWeight': 'bold', 'marginBottom': '10px'},
                ),
                html.Label('Workflow Name'),
                dcc.Dropdown(
                    id='workflow-name',
                    options=workflow_options,
                    value=workflow_id,
                    style={'width': '100%', 'marginBottom': '10px'},
                ),
                # Hidden div to store parameters
                html.Div(
                    id='workflow-params-container',
                    children=[],
                    style={'marginTop': '10px'},
                ),
                html.Button(
                    'Apply Workflow',
                    id='workflow-apply-button',
                    n_clicks=0,
                    style={'width': '100%', 'marginTop': '10px'},
                ),
                # Add hidden div to store the source name this control set is for
                html.Div(
                    id='workflow-source-storage',
                    children=source_name,
                    style={'display': 'none'},
                ),
            ]

            return controls

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
                parameter_widgets.extend(self._create_parameter_widget(param))

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
