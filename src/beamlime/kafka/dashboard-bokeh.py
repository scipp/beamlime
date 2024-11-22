import numpy as np
from bokeh.layouts import row
from bokeh.models import Column, Slider
from bokeh.plotting import curdoc, figure

# Create initial data
points = 100
mean = 0
std = 1
x = np.linspace(0, 10, points)
y = np.random.normal(mean, std, points)
data_2d = np.random.normal(mean, std, (20, 20))

# Create plots
p1 = figure(title="1D Random Data", width=400, height=300)
line = p1.line(x, y, line_width=2)

p2 = figure(title="2D Random Data", width=400, height=300)
heatmap = p2.image(image=[data_2d], x=0, y=0, dw=10, dh=10, palette="Viridis256")

# Create sliders
update_speed = Slider(
    title="Update Speed (ms)", value=1000, start=100, end=5000, step=100
)
num_points = Slider(title="Number of Points", value=points, start=10, end=500, step=10)
mean_slider = Slider(title="Mean", value=mean, start=-5, end=5, step=0.1)
std_slider = Slider(title="Standard Deviation", value=std, start=0.1, end=3, step=0.1)


def update():
    new_y = np.random.normal(mean_slider.value, std_slider.value, num_points.value)
    new_x = np.linspace(0, 10, num_points.value)
    line.data_source.data.update({'x': new_x, 'y': new_y})

    new_2d = np.random.normal(mean_slider.value, std_slider.value, (20, 20))
    heatmap.data_source.data['image'] = [new_2d]


def update_params(attr, old, new):
    # Remove existing callback and create new one with updated speed
    curdoc().remove_periodic_callback(update_callback[0])
    update_callback[0] = curdoc().add_periodic_callback(update, update_speed.value)
    update()


# Store callback in list to allow modification
update_callback = [curdoc().add_periodic_callback(update, update_speed.value)]

# Add slider callbacks
update_speed.on_change('value', update_params)
mean_slider.on_change('value', lambda attr, old, new: update())
std_slider.on_change('value', lambda attr, old, new: update())
num_points.on_change('value', lambda attr, old, new: update())

# Layout
controls = Column(update_speed, num_points, mean_slider, std_slider)
layout = row(controls, p1, p2)
curdoc().add_root(layout)
