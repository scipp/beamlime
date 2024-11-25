import json
import threading

import numpy as np
from bokeh.layouts import row, column
from bokeh.models import Column, Slider
from bokeh.plotting import curdoc, figure
from config_service import ConfigService
from confluent_kafka import Consumer
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

# Dictionary to store monitor plots
monitor_plots = {}

# Create initial 2D plot only (remove p1 creation)
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
            new_2d = ArrayMessage.model_validate_json(msg.value())
            heatmap.data_source.data['image'] = [new_2d]
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error processing Kafka message: {e}")


def create_monitor_plot(key: str):
    """Create a new plot for a monitor with given key."""
    plot = figure(title=f"Monitor {key}", width=400, height=300)
    plot.line([], [], line_width=2, name='data')
    return plot


def check_monitor_update():
    messages = monitor_consumer.consume(num_messages=100, timeout=0.05)
    print(f'Got {len(messages)} messages')
    if not messages:
        return

    updates = {}
    # Process all messages to get latest data for each key
    for msg in messages:
        if msg is not None and not msg.error():
            try:
                key = msg.key().decode('utf-8')  # Decode bytes to string
                data = ArrayMessage.model_validate_json(msg.value()).data
                updates[key] = data
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Error processing Kafka message: {e}")

    layout_modified = False
    # Update or create plots for each key
    for key, data in updates.items():
        if key not in monitor_plots:
            # Create new plot
            monitor_plots[key] = create_monitor_plot(key)
            layout_modified = True

        # Update plot data
        plot = monitor_plots[key]
        new_y = data[: len(data) // 2]
        new_x = data[len(data) // 2 :]
        plot.select_one({'name': 'data'}).data_source.data.update(
            {'x': new_x, 'y': new_y}
        )

    if layout_modified:
        # Rebuild layout
        update_layout()


def update_layout():
    """Update the document layout with all plots."""
    controls = Column(update_speed, num_points)
    monitor_columns = [controls, p2]  # Start with controls and 2D plot

    # Add all monitor plots
    for plot in monitor_plots.values():
        monitor_columns.append(plot)

    layout = row(monitor_columns)

    # Remove old layout and add new one
    while len(curdoc().roots) > 0:
        curdoc().remove_root(curdoc().roots[0])
    curdoc().add_root(layout)


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

# Initial layout setup
update_layout()

# Setup callbacks
update_callback = [curdoc().add_periodic_callback(update, update_speed.value)]
update_speed.on_change('value', update_params)
num_points.on_change('value', publish_request_num_point)
