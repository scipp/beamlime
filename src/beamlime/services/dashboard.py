# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import logging
import threading
import time
from contextlib import ExitStack

import plotly.graph_objects as go
import scipp as sc
from dash import Dash, Input, Output, dcc, html
from dash.exceptions import PreventUpdate

from beamlime import Service, ServiceBase
from beamlime.config import config_names
from beamlime.config.config_loader import load_config
from beamlime.core.config_service import ConfigService
from beamlime.core.message import compact_messages
from beamlime.kafka import consumer as kafka_consumer
from beamlime.kafka.helpers import topic_for_instrument
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
        debug: bool = False,
        log_level: int = logging.INFO,
    ) -> None:
        name = f'{instrument}_dashboard'
        super().__init__(name=name, log_level=log_level)

        self._instrument = instrument
        self._debug = debug

        # Initialize state
        self._monitor_plots: dict[str, go.Figure] = {}
        self._detector_plots: dict[str, go.Figure] = {}

        self._exit_stack = ExitStack()
        self._exit_stack.__enter__()

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
        self._config_service = ConfigService(
            kafka_config={**kafka_downstream_config},
            consumer=self._exit_stack.enter_context(
                kafka_consumer.make_control_consumer(instrument=self._instrument)
            ),
            topic=topic_for_instrument(
                topic='beamlime_control', instrument=self._instrument
            ),
            logger=self._logger,
        )
        self._config_service_thread = threading.Thread(
            target=self._config_service.start
        )

    def _setup_kafka_consumer(self) -> AdaptingMessageSource:
        consumer_config = load_config(
            namespace=config_names.reduced_data_consumer, env=''
        )
        kafka_downstream_config = load_config(namespace=config_names.kafka_downstream)
        config = load_config(namespace=config_names.visualization, env='')
        consumer = self._exit_stack.enter_context(
            kafka_consumer.make_consumer_from_config(
                topics=config['topics'],
                config={**consumer_config, **kafka_downstream_config},
                instrument=self._instrument,
                group='dashboard',
            )
        )
        return AdaptingMessageSource(
            source=KafkaMessageSource(consumer=consumer, num_messages=1000),
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
            ),
        )

    def _setup_layout(self) -> None:
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
            html.Label('Sliding Window (ms)'),
            dcc.Slider(
                id='sliding-window',
                min=8,
                max=13,
                step=1,
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
            html.Label('Time-of-arrival (us)'),
            dcc.Checklist(
                id='toa-checkbox',
                options=[
                    {'label': 'Filter by time-of-arrival [μs]', 'value': 'enabled'}
                ],
                value=[],
                style={'margin': '10px 0'},
            ),
            html.Label('Time-of-arrival Center (us)'),
            dcc.Slider(
                id='toa-center',
                min=0,
                max=71_000,
                step=100,
                value=35_500,
                marks={i: str(i) for i in range(0, 71_001, 10_000)},
            ),
            html.Label('Time-of-arrival Width (us)'),
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
        ]
        self._app.layout = html.Div(
            [
                html.Div(
                    controls,
                    style={'width': '300px', 'float': 'left', 'padding': '10px'},
                ),
                html.Div(id='plots-container', style={'margin-left': '320px'}),
                dcc.Interval(id='interval-component', interval=200, n_intervals=0),
            ]
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
            Output('interval-component', 'interval'),
            Input('update-speed', 'value'),
            Input('sliding-window', 'value'),
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

    def update_roi(self, x_center, x_delta, y_center, y_delta):
        x_min = max(0, x_center - x_delta)
        x_max = min(100, x_center + x_delta)
        y_min = max(0, y_center - y_delta)
        y_max = min(100, y_center + y_delta)

        self._config_service.update_config(
            'roi_x', {'min': x_min / 100, 'max': x_max / 100}
        )
        self._config_service.update_config(
            'roi_y', {'min': y_min / 100, 'max': y_max / 100}
        )

        # Update ROI rectangles in all 2D detector plots
        for fig in self._detector_plots.values():
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
        if len(toa_enabled) == 0:
            self._config_service.update_config('toa_range', None)
        else:
            low = max(0, center - delta)
            high = min(71_000, center + delta)
            self._config_service.update_config(
                'toa_range', {'low': low, 'high': high, 'unit': 'us'}
            )
        return center, delta

    def update_use_weights(self, value: list[str]) -> list[str]:
        self._config_service.update_config('use_weights', len(value) > 0)
        return value

    @staticmethod
    def create_monitor_plot(key: str, data: sc.DataArray) -> go.Figure:
        fig = go.Figure()
        fig.add_scatter(x=[], y=[], mode='lines', line_width=2)
        dim = data.dim
        fig.update_layout(
            title=key,
            width=500,
            height=400,
            xaxis_title=f'{dim} [{data.coords[dim].unit}]',
            yaxis_title=f'[{data.unit}]',
            uirevision=key,
        )
        return fig

    @staticmethod
    def create_detector_plot(key: str, data: sc.DataArray) -> go.Figure:
        if len(data.dims) == 1:
            return DashboardApp.create_monitor_plot(key, data)

        fig = go.Figure()
        y_dim, x_dim = data.dims
        fig.add_heatmap(
            z=[[]],
            x=[],  # Will be filled with coordinate values
            y=[],  # Will be filled with coordinate values
            colorscale='Viridis',
        )
        # Add ROI rectangle (initially hidden)
        fig.add_shape(
            type="rect",
            x0=0,
            y0=0,
            x1=1,
            y1=1,
            line={'color': 'red', 'width': 2},
            fillcolor="red",
            opacity=0.2,
            visible=False,
            name="ROI",
        )

        def maybe_unit(dim: str) -> str:
            unit = data.coords[dim].unit
            return f' [{unit}]' if unit is not None else ''

        size = 800
        opts = {
            'title': key,
            'xaxis_title': f'{x_dim}{maybe_unit(x_dim)}',
            'yaxis_title': f'{y_dim}{maybe_unit(y_dim)}',
            'uirevision': key,
            'showlegend': False,
        }
        y_size, x_size = data.shape
        if y_size < x_size:
            fig.update_layout(width=size, **opts)
            fig.update_yaxes(scaleanchor="x", scaleratio=1, constrain="domain")
            fig.update_xaxes(constrain="domain")
        else:
            fig.update_layout(height=size, **opts)
            fig.update_xaxes(scaleanchor="y", scaleratio=1, constrain="domain")
            fig.update_yaxes(constrain="domain")
        return fig

    def update_plots(self, n: int | None):
        if n is None:
            raise PreventUpdate

        try:
            messages = self._source.get_messages()
            num = len(messages)
            messages = compact_messages(messages)
            self._logger.info(
                "Got %d messages, showing most recent %d", num, len(messages)
            )
            monitor_messages = [
                msg for msg in messages if 'beam_monitor' in msg.key.topic
            ]
            detector_messages = [msg for msg in messages if 'detector' in msg.key.topic]

            for msg in monitor_messages:
                key = msg.key.source_name
                data = msg.value
                if key not in self._monitor_plots:
                    self._monitor_plots[key] = self.create_monitor_plot(key, data)
                fig = self._monitor_plots[key]
                fig.data[0].x = data.coords[data.dim].values
                fig.data[0].y = data.values

            for msg in detector_messages:
                key = msg.key.source_name
                data = msg.value
                for dim in data.dims:
                    if dim not in data.coords:
                        data.coords[dim] = sc.arange(dim, data.sizes[dim], unit=None)
                if key not in self._detector_plots:
                    self._detector_plots[key] = self.create_detector_plot(key, data)
                fig = self._detector_plots[key]
                if len(data.dims) == 1:
                    fig.data[0].x = data.coords[data.dim].values
                    fig.data[0].y = data.values
                else:  # 2D
                    y_dim, x_dim = data.dims
                    fig.data[0].x = data.coords[x_dim].values
                    fig.data[0].y = data.coords[y_dim].values
                    fig.data[0].z = data.values

        except Exception as e:
            self._logger.error("Error in update_plots: %s", e)
            raise PreventUpdate from None

        monitor_graphs = [dcc.Graph(figure=fig) for fig in self._monitor_plots.values()]
        detector_graphs = [
            dcc.Graph(figure=fig) for fig in self._detector_plots.values()
        ]

        return [
            html.Div(monitor_graphs, style={'display': 'flex', 'flexWrap': 'wrap'}),
            html.Div(detector_graphs, style={'display': 'flex', 'flexWrap': 'wrap'}),
        ]

    def update_timing_settings(self, update_speed: float, window_size: float) -> float:
        self._config_service.update_config(
            'update_every', {'value': 2**update_speed, 'unit': 'ms'}
        )
        self._config_service.update_config(
            'sliding_window', {'value': 2**window_size, 'unit': 'ms'}
        )
        return 2**update_speed

    def update_num_points(self, value: int) -> int:
        self._config_service.update_config('time_of_arrival_bins', value)
        return value

    def clear_data(self, n_clicks: int | None) -> int:
        if n_clicks is None or n_clicks == 0:
            raise PreventUpdate
        self._config_service.update_config(
            'start_time', {'value': int(time.time_ns()), 'unit': 'ns'}
        )
        return 0

    def _start_impl(self) -> None:
        self._config_service_thread.start()

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
