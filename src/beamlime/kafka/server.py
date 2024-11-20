# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
# consumer_server.py
import base64
import io
import json
import queue
from threading import Lock, Thread

import matplotlib.pyplot as plt
import numpy as np
from confluent_kafka import Consumer, Producer
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# Shared state
latest_data = None
data_lock = Lock()
msg_queue = queue.Queue()

# Kafka setup
consumer = Consumer(
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'web_consumer_group',
        'auto.offset.reset': 'latest',
    }
)
consumer.subscribe(['topic1'])

producer = Producer({'bootstrap.servers': 'localhost:9092'})

# HTML template with slider
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Array Viewer</title>
    <style>
        .container { max-width: 800px; margin: 0 auto; }
        .slider { width: 100%; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Array Viewer</h1>
        <input type="range" min="5" max="50" value="10" class="slider" id="sizeSlider">
        <div>Array size: <span id="sizeValue">10x10</span></div>
        <img id="arrayImage" src="" width="500" height="500">
    </div>
    <script>
        function updateImage() {
            fetch('/get_image')
                .then(response => response.text())
                .then(data => {
                    document.getElementById('arrayImage').src =
                    'data:image/png;base64,' + data;
                });
        }

        document.getElementById('sizeSlider').oninput = function() {
            let size = parseInt(this.value);
            document.getElementById('sizeValue').textContent = size + 'x' + size;
            fetch('/update_size/' + size, {method: 'POST'});
        };

        setInterval(updateImage, 1000);
    </script>
</body>
</html>
'''


def consume_messages():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            with data_lock:
                global latest_data
                latest_data = np.array(data['data'])
        except Exception as e:
            print(f"Error processing message: {e}")


def generate_image():
    with data_lock:
        if latest_data is None:
            return None

        plt.figure(figsize=(6, 6))
        plt.imshow(latest_data, cmap='viridis')
        plt.colorbar()

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        plt.close()

        buf.seek(0)
        return base64.b64encode(buf.read()).decode('utf-8')


@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route('/get_image')
def get_image():
    image_data = generate_image()
    return image_data if image_data else ''


@app.route('/update_size/<int:size>', methods=['POST'])
def update_size(size):
    control_msg = json.dumps({'size': [size, size]}).encode('utf-8')
    producer.produce('topic1_control', value=control_msg)
    producer.flush()
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    # Start consumer thread
    consumer_thread = Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    # Run Flask app
    app.run(host='127.0.0.1', port=5000)
