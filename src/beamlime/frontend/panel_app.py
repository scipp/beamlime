# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio
import threading

import holoviews as hv
import msgpack
import numpy as np
import panel as pn
import websockets
from holoviews.streams import Pipe

from beamlime.core.serialization import deserialize_data_array

data_pipe = Pipe(data=[])


def websocket_worker():
    """Background thread for WebSocket communication"""

    cnt = 0

    async def connect_websocket():
        nonlocal cnt
        uri = "ws://localhost:5555/ws"
        async with websockets.connect(uri) as websocket:
            while True:
                try:
                    data = await websocket.recv()
                    print(f'new data {cnt}')
                    cnt += 1
                    if data:
                        try:
                            arrays = {
                                tuple(k.split('||')): deserialize_data_array(v)
                                for k, v in msgpack.unpackb(data).items()
                            }
                            data_pipe.send(data=arrays)
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


# This is annoying, as changing these settings resets the view
def get_plots(vmin=0.0, vmax=10.0, log_scale=False, reset=0):
    """Creates heatmap plots with shared color scale settings"""

    def plot_fn(data):
        if not data:
            return hv.Layout([hv.Image([])] * 4).cols(2)
        plots = []
        for key, da in data.items():
            title = ' || '.join(str(k) for k in key)
            xdim = da.dims[1]
            ydim = da.dims[0]
            if xdim in da.coords:
                x = da.coords[xdim].values
            else:
                x = np.arange(da.shape[1])
            if ydim in da.coords:
                y = da.coords[ydim].values
            else:
                y = np.arange(da.shape[0])
            z = da.values
            plot = hv.Image(
                (x, y, z), kdims=[xdim, ydim], vdims=[f'[{da.unit}]'], label=title
            )
            plot.opts(
                cmap='viridis',
                clim=(vmin, vmax),
                colorbar=True,
                title=title,
                width=400,
                height=400,
                aspect='equal',
            )
            if log_scale:
                plot = plot.opts(clim=(1, vmax), logz=True)
            plots.append(plot)
        return hv.Layout(plots).cols(2).opts(shared_axes=False)

    return hv.DynamicMap(plot_fn, streams=[data_pipe])


# Single slider for colorbar max value
vmax_widget = pn.widgets.FloatSlider(
    name="Color scale max", value=2.0, start=0.0, end=100.0, step=1
)
vmin_widget = pn.widgets.FloatSlider(
    name="Color scale min", value=0.0, start=0.0, end=10, step=1
)
# Button for switching between linear and log scale
scale_widget = pn.widgets.Toggle(name="Log scale", value=False)
reset = pn.widgets.Button(name="Reset", button_type="primary")

bound_plot = pn.bind(
    get_plots,
    vmin=vmin_widget,
    vmax=vmax_widget,
    log_scale=scale_widget,
    reset=reset.param.clicks,
)


servable = pn.template.MaterialTemplate(
    site="Panel",
    title="Multiple 2D Heatmaps Demo",
    sidebar=[vmin_widget, vmax_widget, scale_widget, reset],
    main=[bound_plot],
).servable()


def main():
    threading.Thread(target=websocket_worker, daemon=True).start()
    return servable


if __name__ == "__main__":
    pn.serve(main, port=5043, show=True)
