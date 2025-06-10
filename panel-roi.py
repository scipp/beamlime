import time
import numpy as np
import panel as pn
import param
from bokeh.plotting import figure
from bokeh.models import (
    ColorBar,
    LinearColorMapper,
    ColumnDataSource,
    BoxSelectTool,
    LassoSelectTool,
)
from bokeh.palettes import Viridis256

pn.extension("bokeh", sizing_mode="stretch_both")


class ImageDashboard(param.Parameterized):
    width = param.Integer(default=500, bounds=(50, 2000))
    height = param.Integer(default=500, bounds=(50, 2000))
    num_images = param.Integer(default=10, bounds=(5, 50))
    update_rate = param.Number(default=1.0, bounds=(0.1, 5.0))

    def __init__(self, **params):
        super().__init__(**params)
        self.images = []
        self.current_index = 0
        self.last_update_time = 0
        self.update_times = []
        self.plot = None
        self.data_source = None
        self.color_mapper = None
        self.roi_callback = None  # User-defined callback for ROI selection
        self._generate_images()
        self._create_plot()

    def set_roi_callback(self, callback_func):
        """Set a user-defined callback function for ROI selection.

        Parameters
        ----------
        callback_func
            Function that takes (roi_type, roi_data) as parameters.
            roi_type: 'box' or 'lasso'
            roi_data: dict with selection coordinates
        """
        self.roi_callback = callback_func

    def _handle_selection(self, attr, old, new):
        """Handle selection events from Bokeh plot."""
        if not new or not self.roi_callback:
            return

        # Get selection indices
        indices = new
        if not indices:
            return

        # Create a mesh grid for coordinate mapping
        y_coords, x_coords = np.mgrid[0 : self.height, 0 : self.width]

        # Get selected pixel coordinates
        selected_pixels = []
        for idx in indices:
            if idx < len(x_coords.flat):
                x = x_coords.flat[idx]
                y = y_coords.flat[idx]
                selected_pixels.append((x, y))

        if selected_pixels:
            # Determine bounding box
            x_coords = [p[0] for p in selected_pixels]
            y_coords = [p[1] for p in selected_pixels]

            roi_data = {
                'pixels': selected_pixels,
                'bbox': {
                    'x_min': min(x_coords),
                    'x_max': max(x_coords),
                    'y_min': min(y_coords),
                    'y_max': max(y_coords),
                },
                'image_shape': (self.height, self.width),
                'num_pixels': len(selected_pixels),
            }

            # Call user callback
            self.roi_callback('selection', roi_data)

    def _generate_images(self):
        """Pre-generate images to cycle through."""
        print(
            f"Generating {self.num_images} images of size {self.height}x{self.width}..."
        )
        start_time = time.time()

        self.images = []
        for i in range(self.num_images):
            # Create varied patterns for visual feedback
            x = np.linspace(-2, 2, self.width)
            y = np.linspace(-2, 2, self.height)
            X, Y = np.meshgrid(x, y)

            # Different patterns for each image
            if i % 3 == 0:
                img = np.sin(X + i * 0.5) * np.cos(Y + i * 0.3)
            elif i % 3 == 1:
                img = np.exp(-(X**2 + Y**2) / (0.5 + i * 0.1))
            else:
                img = np.sin(np.sqrt(X**2 + Y**2) + i * 0.5)

            # Add some noise for realism
            img += 0.1 * np.random.randn(self.height, self.width)
            self.images.append(img)

        generation_time = time.time() - start_time
        print(f"Generated {self.num_images} images in {generation_time:.2f} seconds")
        self.current_index = 0

    @param.depends('width', 'height', 'num_images', watch=True)
    def _update_images(self):
        """Regenerate images when parameters change."""
        self._generate_images()
        # Recreate plot when dimensions change
        self._create_plot()

    def get_current_image(self):
        """Get current image and advance index."""
        if not self.images:
            return np.zeros((100, 100))

        img = self.images[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.images)
        return img

    def _create_plot(self):
        """Create bokeh plot for the image (called once or when dimensions change)."""
        img = self.get_current_image()

        # Create pixel-level data for selection
        h, w = img.shape
        y_indices, x_indices = np.mgrid[0:h, 0:w]

        # Flatten for ColumnDataSource
        x_flat = x_indices.flatten()
        y_flat = y_indices.flatten()
        values_flat = img.flatten()

        # Create data source with pixel-level data for selection
        self.data_source = ColumnDataSource(
            data=dict(
                image=[img],
                x=[0],
                y=[0],
                dw=[self.width],
                dh=[self.height],
                # Pixel-level data for selection
                x_pixels=x_flat,
                y_pixels=y_flat,
                values=values_flat,
            )
        )

        # Create figure with selection tools
        tools = "pan,wheel_zoom,box_zoom,reset,save,box_select,lasso_select"
        self.plot = figure(
            title=f"Image Display ({self.height}x{self.width}) - Select ROI",
            tools=tools,
            sizing_mode="stretch_both",
        )

        # Color mapper
        self.color_mapper = LinearColorMapper(
            palette=Viridis256, low=img.min(), high=img.max()
        )

        # Image renderer
        image_renderer = self.plot.image(
            image='image',
            x='x',
            y='y',
            dw='dw',
            dh='dh',
            color_mapper=self.color_mapper,
            source=self.data_source,
        )

        # Add invisible scatter plot for selection
        scatter_source = ColumnDataSource(
            data=dict(
                x=x_flat[::10],  # Subsample for performance
                y=y_flat[::10],
                values=values_flat[::10],
            )
        )

        scatter = self.plot.scatter(
            'x',
            'y',
            source=scatter_source,
            size=1,
            alpha=0.0,  # Invisible
            selection_color='red',
            selection_alpha=0.3,
        )

        # Add selection callback
        scatter_source.selected.on_change('indices', self._handle_selection)

        # Color bar
        color_bar = ColorBar(color_mapper=self.color_mapper, width=8, location=(0, 0))
        self.plot.add_layout(color_bar, 'right')

    def update_image_data(self):
        """Update only the image data, preserving plot state."""
        if self.data_source is None or self.plot is None:
            return

        # Get new image
        img = self.get_current_image()

        # Update color mapper range
        self.color_mapper.low = img.min()
        self.color_mapper.high = img.max()

        # Update image data
        h, w = img.shape
        y_indices, x_indices = np.mgrid[0:h, 0:w]
        x_flat = x_indices.flatten()
        y_flat = y_indices.flatten()
        values_flat = img.flatten()

        # Update data source (this preserves zoom/pan state)
        self.data_source.data = dict(
            image=[img],
            x=[0],
            y=[0],
            dw=[self.width],
            dh=[self.height],
            x_pixels=x_flat,
            y_pixels=y_flat,
            values=values_flat,
        )

    def create_plot(self):
        """Return the created plot."""
        return self.plot


# Create dashboard instance
dashboard = ImageDashboard()

# ROI information display
roi_info = pn.pane.HTML("<b>ROI Selection:</b> No region selected")


# Example ROI callback function
def roi_callback(roi_type: str, roi_data: dict):
    """Example callback function for ROI selection."""
    bbox = roi_data['bbox']
    num_pixels = roi_data['num_pixels']

    roi_info.object = (
        f"<b>ROI Selection:</b><br>"
        f"Type: {roi_type}<br>"
        f"Pixels: {num_pixels}<br>"
        f"Bounding box:<br>"
        f"  X: {bbox['x_min']} - {bbox['x_max']}<br>"
        f"  Y: {bbox['y_min']} - {bbox['y_max']}<br>"
        f"Image shape: {roi_data['image_shape']}"
    )

    # You can add your custom processing here
    print(f"ROI selected: {roi_type}, {num_pixels} pixels")


# Set the ROI callback
dashboard.set_roi_callback(roi_callback)

# Create widgets
width_slider = pn.widgets.IntSlider(
    name="Width", start=50, end=2000, step=50, value=500
)
height_slider = pn.widgets.IntSlider(
    name="Height", start=50, end=2000, step=50, value=500
)
num_images_slider = pn.widgets.IntSlider(
    name="Number of Images", start=5, end=50, step=5, value=10
)
update_rate_slider = pn.widgets.FloatSlider(
    name="Update Rate (seconds)", start=0.1, end=5.0, step=0.1, value=1.0
)

# Performance monitoring
performance_text = pn.pane.HTML("<b>Performance:</b> Waiting for updates...")
status_text = pn.pane.HTML("<b>Status:</b> Ready")

# Link widgets to dashboard parameters
dashboard.param.watch(lambda: None, ['width', 'height', 'num_images'])
width_slider.link(dashboard, value='width')
height_slider.link(dashboard, value='height')
num_images_slider.link(dashboard, value='num_images')
update_rate_slider.link(dashboard, value='update_rate')


def update_plot():
    """Update the plot with new image data."""
    start_time = time.time()

    # Update image data only (preserves plot state)
    dashboard.update_image_data()

    # Update performance metrics
    update_time = time.time() - start_time
    dashboard.update_times.append(update_time)

    # Keep only last 20 measurements
    if len(dashboard.update_times) > 20:
        dashboard.update_times = dashboard.update_times[-20:]

    avg_time = np.mean(dashboard.update_times)
    fps = 1.0 / avg_time if avg_time > 0 else 0

    performance_text.object = (
        f"<b>Performance:</b><br>"
        f"Last update: {update_time*1000:.1f} ms<br>"
        f"Average: {avg_time*1000:.1f} ms<br>"
        f"Effective FPS: {fps:.1f}<br>"
        f"Image size: {dashboard.height}x{dashboard.width}"
    )

    status_text.object = (
        f"<b>Status:</b> Image {dashboard.current_index}/{len(dashboard.images)} "
        f"(cycling every {dashboard.update_rate:.1f}s)"
    )

    return dashboard.plot


# Create dynamic plot (create once)
plot_pane = pn.pane.Bokeh(dashboard.create_plot(), sizing_mode='stretch_both')


# Periodic callback to update the plot
def periodic_update():
    """Periodic callback to update the image."""
    start_time = time.time()

    # Update image data only
    dashboard.update_image_data()

    # Update performance metrics
    update_time = time.time() - start_time
    dashboard.update_times.append(update_time)

    if len(dashboard.update_times) > 20:
        dashboard.update_times = dashboard.update_times[-20:]

    avg_time = np.mean(dashboard.update_times)
    fps = 1.0 / avg_time if avg_time > 0 else 0

    performance_text.object = (
        f"<b>Performance:</b><br>"
        f"Last update: {update_time*1000:.1f} ms<br>"
        f"Average: {avg_time*1000:.1f} ms<br>"
        f"Effective FPS: {fps:.1f}<br>"
        f"Image size: {dashboard.height}x{dashboard.width}"
    )

    status_text.object = (
        f"<b>Status:</b> Image {dashboard.current_index}/{len(dashboard.images)} "
        f"(cycling every {dashboard.update_rate:.1f}s)"
    )


# Create periodic callback
pn.state.add_periodic_callback(
    periodic_update, period=int(dashboard.update_rate * 5000)
)


# Update callback period when rate changes
def update_callback_period(event):
    # Remove old callback and add new one with updated period
    pn.state.curdoc.remove_periodic_callback(periodic_update)
    pn.state.add_periodic_callback(periodic_update, period=int(event.new * 1000))


update_rate_slider.param.watch(update_callback_period, 'value')

# Create layout
template = pn.template.FastGridTemplate(
    title="Image Performance Dashboard", sidebar_width=300, prevent_collision=True
)

# Sidebar controls
template.sidebar.extend(
    [
        pn.pane.Markdown("## Image Controls"),
        width_slider,
        height_slider,
        pn.layout.Divider(),
        pn.pane.Markdown("## Animation Controls"),
        num_images_slider,
        update_rate_slider,
        pn.layout.Divider(),
        pn.pane.Markdown("## Region of Interest"),
        roi_info,
        pn.layout.Divider(),
        pn.pane.Markdown("## Performance"),
        performance_text,
        status_text,
        pn.layout.Divider(),
        pn.pane.Markdown(
            "**Instructions:**<br>"
            "• Adjust image size with sliders<br>"
            "• Images cycle automatically<br>"
            "• Use box select or lasso tools for ROI<br>"
            "• Monitor performance metrics<br>"
            "• Test different sizes for performance"
        ),
    ]
)

# Main content
template.main[:, :] = plot_pane

template.servable()
