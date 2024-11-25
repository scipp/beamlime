import json
import threading

import numpy as np
from bokeh.layouts import row
from bokeh.models import Column, Slider
from bokeh.plotting import curdoc, figure
from config_service import ConfigService
from confluent_kafka import Consumer, Producer
from services_faststream import ArrayMessage

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dashboard-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
}

detector_consumer = Consumer(kafka_config)
detector_consumer.subscribe(['beamlime.detector.counts'])
monitor_consumer = Consumer(kafka_config)
monitor_consumer.subscribe(['beamlime.monitor.counts'])

config_service = ConfigService('localhost:9092')
config_service_thread = threading.Thread(target=config_service.start).start()

# Create initial data
points = 100
mean = 0
std = 1
x = np.linspace(0, 10, points)
y = np.random.normal(mean, std, points)
data_2d = np.random.normal(mean, std, (20, 20))

# Create plots
p1 = figure(title="1D Random Data", width=400, height=300)
line = p1.line(x, y, line_width=2)

p2 = figure(title="2D Random Data", width=400, height=300)
heatmap = p2.image(image=[data_2d], x=0, y=0, dw=10, dh=10, palette="Viridis256")

# Create sliders
update_speed = Slider(
    title="Update Speed (ms)", value=1000, start=100, end=5000, step=100
)
num_points = Slider(title="Number of Points", value=points, start=10, end=500, step=10)


def update():
    check_detector_update()
    check_monitor_update()


def check_detector_update():
    # msg = detector_consumer.poll(timeout=0.05)
    messages = detector_consumer.consume(num_messages=10, timeout=0.05)
    print(f'Got {len(messages)} messages')
    if not messages:
        return
    msg = messages[-1]
    print('Detector: ', msg)
    if msg is not None and not msg.error():
        try:
            new_2d = ArrayMessage.deserialize(msg.value())
            heatmap.data_source.data['image'] = [new_2d]
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error processing Kafka message: {e}")


def check_monitor_update():
    messages = monitor_consumer.consume(num_messages=100, timeout=0.05)
    print(f'Got {len(messages)} messages')
    if not messages:
        return
    msg = messages[-1]
    print('Monitor: ', msg)
    if msg is not None and not msg.error():
        try:
            data = ArrayMessage.deserialize(msg.value())
            new_y = data[: len(data) // 2]
            new_x = data[len(data) // 2 :]
            line.data_source.data.update({'x': new_x, 'y': new_y})
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error processing Kafka message: {e}")


def update_params(attr, old, new):
    # Remove existing callback and create new one with updated speed
    curdoc().remove_periodic_callback(update_callback[0])
    update_callback[0] = curdoc().add_periodic_callback(update, update_speed.value)
    update()


def publish_request_num_point(attr, old, new):
    # control_producer.produce('control', json.dumps({'num_points': new}))
    config_service.update_config('monitor-bins', new)


# Store callback in list to allow modification
update_callback = [curdoc().add_periodic_callback(update, update_speed.value)]

# Add slider callbacks
update_speed.on_change('value', update_params)
num_points.on_change('value', publish_request_num_point)

# Layout
controls = Column(update_speed, num_points)
layout = row(controls, p1, p2)
curdoc().add_root(layout)
