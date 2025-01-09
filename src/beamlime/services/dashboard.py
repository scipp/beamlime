# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import argparse
import logging
import threading
import time

import plotly.graph_objects as go
import scipp as sc
from dash import Dash, Input, Output, dcc, html
from dash.exceptions import PreventUpdate

from beamlime import Service, ServiceBase
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
        control_config = load_config(namespace='control_consumer', env='')
        kafka_downstream_config = load_config(namespace='kafka_downstream')
        self._config_service = ConfigService(
            kafka_config={**control_config, **kafka_downstream_config},
            topic=topic_for_instrument(
                topic='beamlime_monitor_data_control', instrument=self._instrument
            ),
            logger=self._logger,
        )
        self._config_service_thread = threading.Thread(
            target=self._config_service.start
        )

    def _setup_kafka_consumer(self) -> AdaptingMessageSource:
        consumer_config = load_config(namespace='reduced_data_consumer', env='')
        kafka_downstream_config = load_config(namespace='kafka_downstream')
        config = load_config(namespace='visualization', env='')
        self._consumer_cm = kafka_consumer.make_consumer_from_config(
            topics=config['topics'],
            config={**consumer_config, **kafka_downstream_config},
            instrument=self._instrument,
            group='dashboard',
        )
        consumer = self._consumer_cm.__enter__()
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

    @staticmethod
    def create_detector_plot(key: str) -> go.Figure:
        fig = go.Figure()
        fig.add_heatmap(z=[[1, 2], [3, 4]], colorscale='Viridis')
        fig.update_layout(title=key, width=500, height=400, uirevision=key)
        return fig

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

    def update_plots(self, n: int | None):
        if n is None:
            raise PreventUpdate

        try:
            monitor_messages = self._source.get_messages()
            num = len(monitor_messages)
            monitor_messages = compact_messages(monitor_messages)
            self._logger.info(
                "Got %d messages, showing most recent %d", num, len(monitor_messages)
            )

            for msg in monitor_messages:
                key = f'{msg.key.topic}_{msg.key.source_name}'
                data = msg.value
                if key not in self._monitor_plots:
                    self._monitor_plots[key] = self.create_monitor_plot(key, data)
                fig = self._monitor_plots[key]
                fig.data[0].x = data.coords[data.dim].values
                fig.data[0].y = data.values

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
        self._consumer_cm.__exit__(None, None, None)


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
