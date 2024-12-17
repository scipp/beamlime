import atexit
import threading

import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from dash.exceptions import PreventUpdate

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

control_config = load_config(namespace='monitor_data', kind='control')
config_service = ConfigService(kafka_config=control_config)
config_service_thread = threading.Thread(target=config_service.start)
config_service_thread.start()

app = Dash(__name__)

# Initialize empty plot storage
monitor_plots: dict[str, go.Figure] = {}
detector_plots: dict[str, go.Figure] = {}

app.layout = html.Div(
    [
        html.Div(
            [
                html.Label('Update Speed (ms)'),
                dcc.Slider(
                    id='update-speed',
                    min=100,
                    max=5000,
                    step=100,
                    value=1000,
                    marks={i: str(i) for i in range(0, 5001, 1000)},
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
            ],
            style={'width': '300px', 'float': 'left', 'padding': '10px'},
        ),
        html.Div(id='plots-container', style={'margin-left': '320px'}),
        dcc.Interval(
            id='interval-component',
            interval=200,  # in milliseconds
            n_intervals=0,
        ),
    ]
)


def create_detector_plot(key: str) -> go.Figure:
    """Create a new Plotly figure for a detector."""
    fig = go.Figure()
    fig.add_heatmap(z=[[1, 2], [3, 4]], colorscale='Viridis')
    # Setting uirevision ensures that the figure is not redrawn on every update, keeping
    # the zoom level and position.
    fig.update_layout(title=key, width=500, height=400, uirevision=key)
    return fig


def create_monitor_plot(key: str) -> go.Figure:
    """Create a new Plotly figure for a monitor."""
    fig = go.Figure()
    fig.add_scatter(x=[], y=[], mode='lines', line_width=2)
    # Setting uirevision ensures that the figure is not redrawn on every update, keeping
    # the zoom level and position.
    fig.update_layout(title=key, width=500, height=400, uirevision=key)
    return fig


consumer_config = load_config(namespace='visualization', kind='consumer')
consumer_kafka_config = consumer_config['kafka']
consumer_kafka_config['group.id'] = 'monitor_data_dashboard'

consumer = kafka_consumer.make_bare_consumer(
    topics=topic_for_instrument(topic=consumer_config['topics'], instrument='dummy'),
    config=consumer_kafka_config,
)
source = AdaptingMessageSource(
    source=KafkaMessageSource(consumer=consumer),
    adapter=ChainedAdapter(first=KafkaToDa00Adapter(), second=Da00ToScippAdapter()),
)


@app.callback(
    Output('plots-container', 'children'), Input('interval-component', 'n_intervals')
)
def update_plots(n):
    if n is None:
        raise PreventUpdate

    try:
        monitor_messages = source.get_messages()

        print(f"Got {len(monitor_messages)} monitor messages")  # noqa: T201
        for msg in monitor_messages:
            key = f'{msg.key.topic}_{msg.key.source_name}'
            data = msg.value
            if key not in monitor_plots:
                monitor_plots[key] = create_monitor_plot(key)
            fig = monitor_plots[key]
            fig.data[0].x = data.coords[data.dim].values
            fig.data[0].y = data.values

    except Exception as e:
        print(f"Error in update_plots: {e}")  # noqa: T201
        raise PreventUpdate from None

    # Create layout
    monitor_graphs = [dcc.Graph(figure=fig) for fig in monitor_plots.values()]
    detector_graphs = [dcc.Graph(figure=fig) for fig in detector_plots.values()]

    return [
        html.Div(monitor_graphs, style={'display': 'flex', 'flexWrap': 'wrap'}),
        html.Div(detector_graphs, style={'display': 'flex', 'flexWrap': 'wrap'}),
    ]


@app.callback(Output('interval-component', 'interval'), Input('update-speed', 'value'))
def update_interval(value):
    return value


@app.callback(Output('num-points', 'value'), Input('num-points', 'value'))
def update_num_points(value):
    config_service.update_config('monitor-bins', value)
    return value


def shutdown():
    print('Shutting down...')  # noqa: T201
    config_service.stop()
    config_service_thread.join()


atexit.register(shutdown)

if __name__ == '__main__':
    app.run_server(debug=True)
