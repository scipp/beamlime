import atexit
import json
import threading
from typing import Dict, Optional

import plotly.graph_objects as go
from backend import ArrayMessage
from config_service import ConfigService
from confluent_kafka import Consumer, KafkaError, TopicPartition
from dash import Dash, Input, Output, dcc, html
from dash.exceptions import PreventUpdate


def assign_partitions(consumer, topic):
    """Assign all partitions of a topic to a consumer."""
    partitions = consumer.list_topics(topic).topics[topic].partitions
    consumer.assign([TopicPartition(topic, p) for p in partitions])


class KafkaManager:
    def __init__(self, config):
        self.config = config
        self.detector_consumer: Optional[Consumer] = None
        self.monitor_consumer: Optional[Consumer] = None
        self._lock = threading.Lock()

    def get_detector_consumer(self):
        with self._lock:
            if self.detector_consumer is None:
                self.detector_consumer = Consumer(self.config)
                self.detector_consumer.subscribe(['beamlime.detector.counts'])
                assign_partitions(self.detector_consumer, 'beamlime.detector.counts')
            return self.detector_consumer

    def get_monitor_consumer(self):
        with self._lock:
            if self.monitor_consumer is None:
                self.monitor_consumer = Consumer(self.config)
                self.monitor_consumer.subscribe(['beamlime.monitor.counts'])
                assign_partitions(self.monitor_consumer, 'beamlime.monitor.counts')
            return self.monitor_consumer

    def close(self):
        with self._lock:
            if self.detector_consumer:
                self.detector_consumer.close()
            if self.monitor_consumer:
                self.monitor_consumer.close()


kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dashboard-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'fetch.min.bytes': 1,
    'session.timeout.ms': 6000,
    'heartbeat.interval.ms': 2000,
}

kafka_manager = KafkaManager(kafka_config)
config_service = ConfigService('localhost:9092')
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


@app.callback(
    Output('plots-container', 'children'), Input('interval-component', 'n_intervals')
)
def update_plots(n):
    if n is None:
        raise PreventUpdate

    try:
        # Update detector plots
        detector_consumer = kafka_manager.get_detector_consumer()
        detector_messages = detector_consumer.consume(num_messages=10, timeout=0.05)

        print(f"Got {len(detector_messages)} detector messages")
        for msg in detector_messages:
            if msg is None or msg.error():
                if msg and msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(
                    f"Detector consumer error: {msg.error() if msg else 'Unknown error'}"
                )
                continue

            try:
                key = msg.key().decode('utf-8')
                data = ArrayMessage.from_msgpack(msg.value()).data
                if key not in detector_plots:
                    detector_plots[key] = create_detector_plot(key)
                fig = detector_plots[key]
                fig.data[0].z = data
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Error processing detector message: {e}")

        # Update monitor plots
        monitor_consumer = kafka_manager.get_monitor_consumer()
        monitor_messages = monitor_consumer.consume(num_messages=100, timeout=0.05)

        print(f"Got {len(monitor_messages)} monitor messages")
        for msg in monitor_messages:
            if msg is None or msg.error():
                if msg and msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(
                    f"Monitor consumer error: {msg.error() if msg else 'Unknown error'}"
                )
                continue

            try:
                key = msg.key().decode('utf-8')
                data = ArrayMessage.from_msgpack(msg.value()).data
                if key not in monitor_plots:
                    monitor_plots[key] = create_monitor_plot(key)
                fig = monitor_plots[key]
                new_y = data[: len(data) // 2]
                new_x = data[len(data) // 2 :]
                fig.data[0].x = new_x
                fig.data[0].y = new_y
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Error processing monitor message: {e}")

    except Exception as e:
        print(f"Error in update_plots: {e}")
        raise PreventUpdate

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
    print('Shutting down...')
    kafka_manager.close()
    config_service.stop()
    config_service_thread.join()


atexit.register(shutdown)

if __name__ == '__main__':
    app.run_server(debug=True)
