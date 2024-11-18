# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
import threading
from collections import deque

import holoviews as hv
import msgpack
import numpy as np
import pandas as pd
import panel as pn
import param
import websockets
from holoviews.streams import Stream

from beamlime.core.serialization import deserialize_data_array

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
                        print('new data')
                        try:
                            arrays = {
                                tuple(k.split('||')): deserialize_data_array(v)
                                for k, v in msgpack.unpackb(data).items()
                            }
                            data_buffer.append(arrays)
                        except (msgpack.UnpackException, KeyError) as e:
                            print("Error processing data: %s", e)
                except websockets.ConnectionClosed:
                    print("WebSocket connection closed, attempting to reconnect...")
                    await asyncio.sleep(1)
                    break

    async def keep_alive():
        while True:
            try:
                await connect_websocket()
            except Exception as e:
                print(f"WebSocket error: {e}")
                await asyncio.sleep(1)

    asyncio.run(keep_alive())


pn.extension(design="material")


# Define a custom stream for updating data
class DataStream(Stream):
    data_dict = param.Dict(default={})


# Create stream instance
data_stream = DataStream(
    data_dict={
        'a': np.zeros((10, 10)),
        'b': np.zeros((10, 10)),
        'c': np.zeros((10, 10)),
        'd': np.zeros((10, 10)),
    }
)


async def update_data():
    """Update the plot data through ZMQ streaming"""
    if data_buffer:
        try:
            data_dict = {k: v.values for k, v in data_buffer[-1].items()}
            data_stream.event(data_dict=data_dict)
        except (msgpack.UnpackException, KeyError) as e:
            print(f"Error processing data: {e}")


def get_plots(vmin=0.0, vmax=10.0, log_scale=False):
    """Creates heatmap plots with shared color scale settings"""

    def plot_fn(data_dict):
        plots = []
        for key, data in data_dict.items():
            title = ' || '.join(str(k) for k in key)
            plot = hv.Image(data, label=title).opts(
                cmap='viridis',
                height=400,
                width=400,
                clim=(vmin, vmax),
                colorbar=True,
                title=title,
            )
            if log_scale:
                plot = plot.opts(clim=(1, vmax), logz=True)
            plots.append(plot)

        # Arrange plots in a grid layout
        return hv.Layout(plots).cols(2).opts(shared_axes=False)

    return hv.DynamicMap(plot_fn, streams=[data_stream])


# Set up periodic callback
periodic_callback = pn.state.add_periodic_callback(update_data, period=333)

# Single slider for colorbar max value
vmax_widget = pn.widgets.FloatSlider(
    name="Color scale max", value=2.0, start=0.0, end=100.0, step=1
)
vmin_widget = pn.widgets.FloatSlider(
    name="Color scale min", value=0.0, start=0.0, end=10, step=1
)
# Button for switching between linear and log scale
scale_widget = pn.widgets.Toggle(name="Log scale", value=False)

bound_plot = pn.bind(
    get_plots, vmin=vmin_widget, vmax=vmax_widget, log_scale=scale_widget
)

servable = pn.template.MaterialTemplate(
    site="Panel",
    title="Multiple 2D Heatmaps Demo",
    sidebar=[vmin_widget, vmax_widget, scale_widget],
    main=[bound_plot],
).servable()


def main():
    threading.Thread(target=websocket_worker, daemon=True).start()
    return servable


if __name__ == "__main__":
    pn.serve(main, port=5043, show=True)
