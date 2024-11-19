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
import websockets
from flask import Flask, Response, render_template_string, request
from matplotlib.figure import Figure

from beamlime.core.serialization import deserialize_data_array
from beamlime.plotting.plot_matplotlib import MatplotlibPlotter

app = Flask(__name__)

# Thread-safe buffer for latest array
data_buffer = deque(maxlen=1)  # Only keep most recent array


def websocket_worker():
    """Background thread for WebSocket communication"""

    async def connect_websocket():
        uri = "ws://localhost:5555/ws"
        async with websockets.connect(uri) as websocket:
            while True:
                try:
                    data = await websocket.recv()
                    if data:
                        try:
                            arrays = {
                                tuple(k.split('||')): deserialize_data_array(v)
                                for k, v in msgpack.unpackb(data).items()
                            }
                            data_buffer.append(arrays)
                        except (msgpack.UnpackException, KeyError) as e:
                            app.logger.error("Error processing data: %s", e)
                except websockets.ConnectionClosed:  # noqa: PERF203
                    app.logger.error(
                        "WebSocket connection closed, attempting to reconnect..."
                    )
                    await asyncio.sleep(1)
                    break

    async def keep_alive():
        while True:
            try:
                await connect_websocket()
            except Exception as e:  # noqa: PERF203
                app.logger.error("WebSocket error: %s", e)
                await asyncio.sleep(1)

    asyncio.run(keep_alive())


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

                setInterval(updateImage, 1000);
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


def main():
    threading.Thread(target=websocket_worker, daemon=True).start()
    port = int(os.environ.get('FLASK_PORT', 5044))
    app.run(port=port)


if __name__ == "__main__":
    main()
