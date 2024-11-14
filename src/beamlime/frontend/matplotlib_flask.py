# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
"""
Demo of a simple Flask app that plots live data from a ZMQ server using Matplotlib.
"""

import asyncio
import os
import threading
from collections import deque
from io import BytesIO

import msgpack
from flask import Flask, Response, render_template_string, request
from matplotlib.figure import Figure

from beamlime.core.serialization import deserialize_data_array
from beamlime.frontend.zmq_client import ZMQClient, ZMQConfig
from beamlime.plotting.plot_matplotlib import MatplotlibPlotter

app = Flask(__name__)

# Thread-safe buffer for latest array
data_buffer = deque(maxlen=1)  # Only keep most recent array


def zmq_worker():
    """Background thread for ZMQ communication"""

    async def run_client():
        config = ZMQConfig(server_address="tcp://localhost:5555", timeout_ms=1000)
        async with ZMQClient(config).session() as client:
            while True:
                data = await client.receive_latest()
                if data:
                    try:
                        arrays = {
                            tuple(k.split('||')): deserialize_data_array(v)
                            for k, v in msgpack.unpackb(data).items()
                        }
                        data_buffer.append(arrays)
                    except (msgpack.UnpackException, KeyError) as e:
                        print(f"Error processing data: {e}")
                await asyncio.sleep(0.1)

    asyncio.run(run_client())


@app.route("/")
def index():
    return render_template_string("""
    <html>
        <head>
            <title>Live ZMQ Plot</title>
            <script type="text/javascript">
                var scaleType = 'linear';

                function toggleScale() {
                    scaleType = (scaleType === 'linear') ? 'log' : 'linear';
                    document.getElementById('toggleButton').innerText =
                        'Switch to ' + (scaleType === 'linear' ? 'Log' : 'Linear') + ' Scale';
                    updateImage();
                }

                function updateImage() {
                    var img = document.getElementById('plot');
                    img.src = '/plot.png?scale=' + scaleType + '&_t=' + new Date().getTime();
                }

                setInterval(updateImage, 500);
            </script>
        </head>
        <body>
            <h1>Live ZMQ Data Plot</h1>
            <button id="toggleButton" onclick="toggleScale()">Switch to Log Scale</button>
            <br><br>
            <img id="plot" src="/plot.png?scale=linear">
        </body>
    </html>
    """)  # noqa: E501


plotter = MatplotlibPlotter()


@app.route("/plot.png")
def plot_png():
    scale = request.args.get('scale', 'linear')

    if not data_buffer:
        # Return empty plot if no data
        fig = Figure(figsize=(10, 6))
        ax = fig.subplots()
        ax.text(0.5, 0.5, 'Waiting for data...', ha='center', va='center')
    else:
        plotter.update_data(data_buffer[-1], norm=scale)
        fig = plotter.fig

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=50)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="image/png")


if __name__ == "__main__":
    # Start ZMQ client thread
    threading.Thread(target=zmq_worker, daemon=True).start()
    port = int(os.environ.get('FLASK_PORT', 5042))
    app.run(debug=True, port=port)
