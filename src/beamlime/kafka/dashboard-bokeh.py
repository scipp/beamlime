import atexit
import json
import threading

from bokeh.layouts import row
from bokeh.models import Column, Slider
from bokeh.plotting import curdoc, figure
from config_service import ConfigService
from confluent_kafka import Consumer
from backend import ArrayMessage

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dashboard-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'fetch.min.bytes': 1,
}

detector_consumer = Consumer(kafka_config)
detector_consumer.subscribe(['beamlime.detector.counts'])
monitor_consumer = Consumer(kafka_config)
monitor_consumer.subscribe(['beamlime.monitor.counts'])

config_service = ConfigService('localhost:9092')
config_service_thread = threading.Thread(target=config_service.start)
config_service_thread.start()

monitor_plots = {}
detector_plots = {}

update_speed = Slider(
    title="Update Speed (ms)", value=1000, start=100, end=5000, step=100
)
num_points = Slider(title="Time-of-arrival bins", value=100, start=10, end=500, step=10)


def update():
    check_detector_update()
    check_monitor_update()


def create_detector_plot(key: str):
    """Create a new plot for a detector with given key."""
    plot = figure(title=f"{key}", width=400, height=300)
    plot.image(image=[], x=0, y=0, dw=10, dh=10, palette="Viridis256", name='data')
    return plot


def check_detector_update():
    messages = detector_consumer.consume(num_messages=10, timeout=0.05)
    print(f'Got {len(messages)} detector messages')
    if not messages:
        return

    updates = {}
    # Process all messages to get latest data for each key
    for msg in messages:
        if msg is not None and not msg.error():
            try:
                key = msg.key().decode('utf-8')
                data = ArrayMessage.model_validate_json(msg.value()).data
                updates[key] = data
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Error processing Kafka message: {e}")

    layout_modified = False
    # Update or create plots for each key
    for key, data in updates.items():
        if key not in detector_plots:
            # Create new plot
            detector_plots[key] = create_detector_plot(key)
            layout_modified = True

        # Update plot data
        plot = detector_plots[key]
        plot.select_one({'name': 'data'}).data_source.data.update({'image': [data]})

    if layout_modified:
        update_layout()


def create_monitor_plot(key: str):
    """Create a new plot for a monitor with given key."""
    plot = figure(title=f"{key}", width=400, height=300)
    plot.line([], [], line_width=2, name='data')
    return plot


def check_monitor_update():
    messages = monitor_consumer.consume(num_messages=100, timeout=0.05)
    print(f'Got {len(messages)} monitor messages')
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
        update_layout()


def update_layout():
    """Update the document layout with controls sidebar and main plot area."""
    # Create sidebar with controls
    sidebar = Column(update_speed, num_points, width=300)
    # Main area with plots
    monitor_row = row(list(monitor_plots.values()))
    detector_row = row(list(detector_plots.values()))
    main_content = Column(monitor_row, detector_row)

    layout = row(sidebar, main_content)

    # Update document
    while len(curdoc().roots) > 0:
        curdoc().remove_root(curdoc().roots[0])
    curdoc().add_root(layout)


def update_params(attr, old, new):
    # Remove existing callback and create new one with updated speed
    curdoc().remove_periodic_callback(update_callback[0])
    update_callback[0] = curdoc().add_periodic_callback(update, update_speed.value)
    update()


def publish_request_num_point(attr, old, new):
    config_service.update_config('monitor-bins', new)


# Store callback in list to allow modification
update_callback = [curdoc().add_periodic_callback(update, update_speed.value)]

# Add slider callbacks
update_speed.on_change('value', update_params)
num_points.on_change('value', publish_request_num_point)

# Initial layout setup
update_layout()


def shutdown():
    # Note: This is not quite what we want, need to interrupt twice for actual shutdown.
    # curdoc().add_periodic_callback is not doing what we want either.
    print('Shutting down...')
    detector_consumer.close()
    monitor_consumer.close()
    config_service.stop()
    config_service_thread.join()


atexit.register(shutdown)
