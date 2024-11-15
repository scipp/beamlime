# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import holoviews as hv
import msgpack
import numpy as np
import pandas as pd
import panel as pn
import param
from holoviews.streams import Stream

from beamlime.core.serialization import deserialize_data_array
from beamlime.frontend.zmq_client import ZMQClient, ZMQConfig


# Global session holder
class ZMQSessionManager:
    def __init__(self):
        self.session = None
        self.zmq_config = ZMQConfig(
            server_address="tcp://localhost:5555", timeout_ms=1000
        )
        self.client = None

    async def start(self):
        if self.client is None:
            self.client = ZMQClient(self.zmq_config)
            await self.client.start()

    async def stop(self):
        if self.client is not None:
            await self.client.stop()
            self.client = None

    async def receive_latest(self):
        if self.client is not None:
            return await self.client.receive_latest()
        return None


# Create global instance
session_manager = ZMQSessionManager()

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
    if session_manager.client is None:
        await session_manager.start()

    data = await session_manager.receive_latest()
    if data:
        try:
            arrays = {
                tuple(k.split('||')): deserialize_data_array(v)
                for k, v in msgpack.unpackb(data).items()
            }
            # Store all arrays
            data_dict = {k: v.values for k, v in arrays.items()}
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
        return hv.Layout(plots).cols(2)

    return hv.DynamicMap(plot_fn, streams=[data_stream])


# Set up periodic callback
periodic_callback = pn.state.add_periodic_callback(update_data, period=100)

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
    return servable


if __name__ == "__main__":
    pn.serve(main, port=5043, show=True)
