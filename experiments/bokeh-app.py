# File: main.py
import asyncio

import numpy as np
from bokeh.layouts import column
from bokeh.plotting import figure, curdoc
from bokeh.models import ColumnDataSource, LinearColorMapper
from bokeh.transform import transform
from bokeh.models import Slider
from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop
from bokeh.server.server import Server
from bokeh.application import Application as BkApplication
from bokeh.application.handlers import FunctionHandler
import json
import msgpack

# Initialize data
N = 200
x = np.linspace(0, 10, N)
y = np.linspace(0, 10, N)
X, Y = np.meshgrid(x, y)
Z = np.sin(X) * np.cos(Y)

source = ColumnDataSource(data=dict(image=[Z]))
color_mapper = LinearColorMapper(palette="Viridis256", low=np.min(Z), high=np.max(Z))
plot = figure(title="Dynamic 2D Heatmap", x_range=(0, 10), y_range=(0, 10))
plot.image(
    image='image', x=0, y=0, dw=10, dh=10, color_mapper=color_mapper, source=source
)


def update():
    Z_new = np.sin(X + np.random.rand()) * np.cos(update.scale * (Y + np.random.rand()))
    source.data = dict(image=[Z_new])


update.scale = 2.0


# Add slider for scale control
scale_slider = Slider(title="Scale", value=2.0, start=0.1, end=5.0, step=0.1)


# Modify update function to use slider value
def update_scale(attr, old, new):
    update.scale = new
    # update(scale=new)


scale_slider.on_change('value', update_scale)

# Update layout to include slider
# curdoc().add_root(column(scale_slider, plot))


# Add periodic callback
# curdoc().add_periodic_callback(update, 200)

root_doc = None


# Create the Bokeh application
def modify_doc(doc):
    doc.add_root(column(scale_slider, plot))
    # doc.add_periodic_callback(update, 200)
    global root_doc
    root_doc = doc


# Define the HTTP handler for POST requests
class DataHandler(RequestHandler):
    def post(self):
        try:
            print('hi')
            # data = json.loads(self.request.body)
            data = msgpack.unpackb(self.request.body, raw=False)
            Z_new = np.array(data['Z'])

            def update_source():
                source.data = dict(image=[Z_new])

            root_doc.add_next_tick_callback(update_source)
            # self.write({"status": "success"})
        except Exception as e:
            self.write({"status": "error", "message": str(e)})

    def get(self):
        self.write(f'{scale_slider.value}')


bokeh_app = BkApplication(FunctionHandler(modify_doc))

# Start the server
# server = Server(tornado_app, port=5556)
server = Server(
    {'/bokeh': bokeh_app},
    io_loop=IOLoop.current(),
    port=5556,
    allow_websocket_origin=["localhost:5556"],
    extra_patterns=[(r'/update_data', DataHandler), (r'/get_scale', DataHandler)],
    # http_server_kwargs={'extra_patterns': [(r'/update_data', DataHandler)]},
)
server.start()
server.io_loop.add_callback(server.show, "/bokeh")
server.io_loop.start()
