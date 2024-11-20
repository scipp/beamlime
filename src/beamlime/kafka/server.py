# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# consumer_server.py
import base64
import io
import json
import queue
from threading import Lock, Thread
from uuid import uuid4

import matplotlib.pyplot as plt
import numpy as np
from confluent_kafka import Consumer, Producer
from flask import Flask, jsonify, render_template_string
from server_html import HTML_TEMPLATE

app = Flask(__name__)

latest_data = {}
data_lock = Lock()
msg_queue = queue.Queue()

consumer = Consumer(
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': f'web_consumer_group_{uuid4()}',
        'auto.offset.reset': 'latest',
    }
)
consumer.subscribe([f'sensor_data_{i}' for i in range(4)])

producer = Producer({'bootstrap.servers': 'localhost:9092'})


def consume_messages():
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            with data_lock:
                global latest_data
                latest_data[msg.topic()] = np.array(data['data'])
        except Exception as e:
            print(f"Error processing message: {e}")


@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route('/update_size/<int:size>', methods=['POST'])
def update_size(size):
    control_msg = json.dumps({'size': [size, size]}).encode('utf-8')
    producer.produce('beamlime-control', value=control_msg)
    producer.flush()
    return jsonify({'status': 'ok'})


@app.route('/get_images')
def get_images():
    return jsonify(generate_images())


_figures = {}
_axes = {}


def initialize_plot(topic, data):
    _figures[topic] = plt.figure(figsize=(6, 6))
    _axes[topic] = _figures[topic].add_subplot(111)
    img = _axes[topic].imshow(data, cmap='viridis')
    _figures[topic].colorbar(img)
    _axes[topic].set_title(topic)
    _axes[topic].images[0].set_clim(data.min(), data.max())


def generate_images():
    with data_lock:
        if not latest_data:
            return {}
        data_copy = latest_data.copy()

    images = {}
    for topic, data in data_copy.items():
        if topic not in _figures:
            initialize_plot(topic, data)
        else:
            _axes[topic].images[0].set_array(data)
            _axes[topic].images[0].set_clim(data.min(), data.max())

        # Save to buffer
        buf = io.BytesIO()
        _figures[topic].canvas.draw()
        _figures[topic].savefig(buf, format='png')
        buf.seek(0)
        images[topic] = base64.b64encode(buf.read()).decode('utf-8')

    return images


def cleanup_plots():
    """Clean up matplotlib resources"""
    for fig in _figures.values():
        plt.close(fig)
    _figures.clear()
    _axes.clear()


if __name__ == '__main__':
    consumer_thread = Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    app.run(host='127.0.0.1', port=5001)
