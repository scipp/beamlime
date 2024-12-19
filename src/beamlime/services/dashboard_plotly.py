# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import logging
import threading
import time
import uuid

import plotly.graph_objects as go
import scipp as sc
from dash import Dash, Input, Output, dcc, html
from dash.exceptions import PreventUpdate

from beamlime import Service, ServiceBase
from beamlime.config.config_loader import load_config
from beamlime.core.config_service import ConfigService
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

    def _setup_config_service(self) -> None:
        control_config = load_config(namespace='monitor_data')['control']
        self._config_service = ConfigService(kafka_config=control_config)
        self._config_service_thread = threading.Thread(
            target=self._config_service.start
        )

    def _setup_kafka_consumer(self) -> AdaptingMessageSource:
        consumer_config = load_config(namespace='visualization')['consumer']
        consumer_kafka_config = consumer_config['kafka']
        consumer_kafka_config['group.id'] = (
            f'{self._instrument}_dashboard_{uuid.uuid4()}'
        )

        consumer = kafka_consumer.make_bare_consumer(
            topics=topic_for_instrument(
                topic=consumer_config['topics'], instrument=self._instrument
            ),
            config=consumer_kafka_config,
        )
        return AdaptingMessageSource(
            source=KafkaMessageSource(consumer=consumer),
            adapter=ChainedAdapter(
                first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()
            ),
        )

    def _setup_layout(self) -> None:
        self._app.layout = html.Div(
            [
                html.Div(
                    [
                        html.Label('Update Speed (ms)'),
                        dcc.Slider(
                            id='update-speed',
                            min=8,
                            max=13,
                            step=0.5,
                            value=10,
                            marks={i: {'label': f'{2**i}'} for i in range(8, 14)},
                        ),
                        html.Label('Time-of-arrival bins'),
                        dcc.Slider(
                            id='num-points',
                            min=10,
                            max=500,
                            step=10,
                            value=100,
                            marks={i: str(i) for i in range(0, 501, 100)},
                        ),
                        html.Button('Clear', id='clear-button', n_clicks=0),
                    ],
                    style={'width': '300px', 'float': 'left', 'padding': '10px'},
                ),
                html.Div(id='plots-container', style={'margin-left': '320px'}),
                dcc.Interval(
                    id='interval-component',
                    interval=200,
                    n_intervals=0,
                ),
            ]
        )

    def _setup_callbacks(self) -> None:
        self._app.callback(
            Output('plots-container', 'children'),
            Input('interval-component', 'n_intervals'),
        )(self.update_plots)

        self._app.callback(
            Output('interval-component', 'interval'), Input('update-speed', 'value')
        )(self.update_interval)

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
            self._logger.info("Got %d monitor messages", len(monitor_messages))

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

    def update_interval(self, value: float) -> float:
        self._config_service.update_config(
            'update_every', {'value': 2**value, 'unit': 'ms'}
        )
        return 2**value

    def update_num_points(self, value: int) -> int:
        self._config_service.update_config('monitor-bins', value)
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
        self._app.run_server(debug=self._debug)

    def _stop_impl(self) -> None:
        """Clean shutdown of all components."""
        self._config_service.stop()
        self._config_service_thread.join()
        self._source.close()


def main() -> None:
    parser = Service.setup_arg_parser(description='Beamlime Dashboard')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    dashboard = DashboardApp(**vars(parser.parse_args()))
    dashboard.start()


if __name__ == '__main__':
    main()
