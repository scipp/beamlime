import holoviews as hv
import numpy as np
import panel as pn
import param
from holoviews import opts, streams

pn.extension('holoviews', template='material')
hv.extension('bokeh')


class DashboardApp(param.Parameterized):
    """Main dashboard application with tab-dependent sidebar controls."""

    # Common controls (always visible)
    sample_name = param.String(default="Sample_001", doc="Sample name")
    run_number = param.Integer(default=12345, doc="Run number")

    # Active tab tracker
    active_tab = param.String(default="Detectors")

    # Tab-specific parameters
    detector_view_mode = param.Selector(
        default="Current", objects=["Current", "Cumulative"], doc="Detector view mode"
    )
    detector_threshold = param.Number(
        default=0.5, bounds=(0, 1), doc="Detection threshold"
    )
    pulse = param.Integer(default=100, bounds=(10, 1000), doc="Number of bins")

    # Polygon data storage
    polygon_data = param.List(default=[], doc="Polygon vertices data")

    monitor_integration_time = param.Number(
        default=10.0, bounds=(0.1, 100), doc="Integration time (s)"
    )
    monitor_normalization = param.Boolean(default=True, doc="Apply normalization")

    reduction_algorithm = param.Selector(
        default="Standard",
        objects=["Standard", "Advanced", "Custom"],
        doc="Reduction algorithm",
    )
    background_subtraction = param.Boolean(default=False, doc="Subtract background")

    def __init__(self, **params):
        super().__init__(**params)
        self.active_tab = "Detectors"
        self._setup_detector_streams()
        self._setup_monitor_streams()
        self._setup_reduction_streams()

        # Initialize polygon display widget
        self._polygon_display = pn.pane.Markdown("No polygons drawn", height=150)

    def _setup_detector_streams(self):
        """Initialize streams for detector data."""
        self._detector_image_pipe = streams.Pipe(data=dict())
        self._detector_histogram_pipe = streams.Pipe(data=dict())

        # Initialize with default data
        self._update_detector_streams()

    def _setup_monitor_streams(self):
        """Initialize streams for monitor data."""
        self._monitor_timeseries_pipe = streams.Pipe(data=dict())
        self._monitor_profile_pipe = streams.Pipe(data=dict())

        # Initialize with default data
        self._update_monitor_streams()

    def _setup_reduction_streams(self):
        """Initialize streams for reduction data."""
        self._reduction_comparison_pipe = streams.Pipe(data=dict())
        self._reduction_residuals_pipe = streams.Pipe(data=dict())

        # Initialize with default data
        self._update_reduction_streams()

    @pn.depends('detector_view_mode', 'pulse', watch=True)
    def _update_detector_streams(self):
        """Update the streams for detector visualizations."""
        # Generate data based on view mode
        x = np.linspace(0, 10, 50)
        y = np.linspace(0, 10, 50)
        X, Y = np.meshgrid(x, y)

        if self.detector_view_mode == "Cumulative":
            Z = 2 * np.sin(X) * np.cos(Y) + np.random.normal(0, 0.05, X.shape)
            counts = np.random.poisson(200, 1000)
        else:
            Z = np.sin(X) * np.cos(Y) + np.random.normal(0, 0.1, X.shape)
            counts = np.random.poisson(100, 1000)

        # Update image stream
        image_data = {'x': x, 'y': y, 'z': Z, 'view_mode': self.detector_view_mode}
        self._detector_image_pipe.send(image_data)

        # Update histogram stream
        hist, edges = np.histogram(counts, bins=self.pulse)
        bin_centers = (edges[:-1] + edges[1:]) / 2

        histogram_data = {
            'counts': bin_centers,
            'frequency': hist,
            'view_mode': self.detector_view_mode,
        }
        self._detector_histogram_pipe.send(histogram_data)

    def _update_monitor_streams(self):
        """Update the streams for monitor visualizations."""
        # Time series data
        time = np.linspace(0, 100, 1000)
        signal = np.sin(0.1 * time) + 0.1 * np.random.randn(1000)

        timeseries_data = {'time': time, 'signal': signal}
        self._monitor_timeseries_pipe.send(timeseries_data)

        # Beam profile data
        x = np.linspace(-5, 5, 100)
        profile = np.exp(-(x**2) / 2) + 0.05 * np.random.randn(100)

        profile_data = {'position': x, 'intensity': profile}
        self._monitor_profile_pipe.send(profile_data)

    def _update_reduction_streams(self):
        """Update the streams for reduction visualizations."""
        # Before/after comparison
        x = np.linspace(0, 20, 200)
        raw_data = np.sin(x) + 0.3 * np.random.randn(200)
        processed_data = np.convolve(raw_data, np.ones(5) / 5, mode='same')

        comparison_data = {
            'q': x,
            'raw_intensity': raw_data,
            'processed_intensity': processed_data,
        }
        self._reduction_comparison_pipe.send(comparison_data)

        # Residuals
        residuals = raw_data - processed_data

        residuals_data = {'q': x, 'residual': residuals}
        self._reduction_residuals_pipe.send(residuals_data)

    def _create_detector_image_plot(self, data):
        """Create detector image plot from stream data."""
        if not data:
            return hv.Image([])

        view_suffix = f" ({data['view_mode']})" if 'view_mode' in data else ""
        title = f"Detector Image{view_suffix}"

        image = hv.Image((data['x'], data['y'], data['z']))
        return image.opts(
            title=title,
            width=400,
            height=300,
            xlabel="X (mm)",
            ylabel="Y (mm)",
            cmap='viridis',
            colorbar=True,
        )

    def _create_detector_histogram_plot(self, data):
        """Create detector histogram plot from stream data."""
        if not data:
            return hv.Histogram([])

        view_suffix = f" ({data['view_mode']})" if 'view_mode' in data else ""
        title = f"Intensity Distribution{view_suffix}"

        histogram = hv.Histogram((data['counts'], data['frequency']))
        return histogram.opts(
            title=title,
            width=400,
            height=300,
            xlabel="Counts",
            ylabel="Frequency",
            color='navy',
            alpha=0.7,
        )

    def _create_monitor_timeseries_plot(self, data):
        """Create monitor time series plot from stream data."""
        if not data:
            return hv.Curve([])

        curve = hv.Curve((data['time'], data['signal']))
        return curve.opts(
            title="Monitor Signal vs Time",
            width=400,
            height=300,
            xlabel="Time (s)",
            ylabel="Signal",
            color='blue',
            line_width=2,
        )

    def _create_monitor_profile_plot(self, data):
        """Create monitor beam profile plot from stream data."""
        if not data:
            return hv.Curve([])

        curve = hv.Curve((data['position'], data['intensity']))
        return curve.opts(
            title="Beam Profile",
            width=400,
            height=300,
            xlabel="Position (mm)",
            ylabel="Intensity",
            color='red',
            line_width=2,
        )

    def _create_reduction_comparison_plot(self, data):
        """Create reduction comparison plot from stream data."""
        if not data:
            return hv.Overlay([])

        raw_curve = hv.Curve((data['q'], data['raw_intensity']), label='Raw')
        processed_curve = hv.Curve(
            (data['q'], data['processed_intensity']), label='Processed'
        )

        overlay = raw_curve * processed_curve
        return overlay.opts(
            title="Data Processing Comparison",
            width=400,
            height=300,
            xlabel="Q (Å⁻¹)",
            ylabel="Intensity",
            legend_position='top_right',
        ).opts(
            hv.opts.Curve('Raw', color='gray', alpha=0.7, line_width=2),
            hv.opts.Curve('Processed', color='green', line_width=2),
        )

    def _create_reduction_residuals_plot(self, data):
        """Create reduction residuals plot from stream data."""
        if not data:
            return hv.Curve([])

        residuals_curve = hv.Curve((data['q'], data['residual']))
        zero_line = hv.Curve((data['q'], np.zeros_like(data['q'])))

        overlay = residuals_curve * zero_line
        return overlay.opts(
            title="Processing Residuals",
            width=400,
            height=300,
            xlabel="Q (Å⁻¹)",
            ylabel="Residual",
        ).opts(
            hv.opts.Curve('Curve', color='orange', line_width=2),
            hv.opts.Curve('Curve.I', color='black', line_dash='dashed', line_width=1),
        )

    def _format_polygon_vertices(self):
        """Format polygon vertices for display."""
        if not self.polygon_data:
            return "No polygons drawn"

        content = "### Polygon Vertices\n\n"
        for i, polygon in enumerate(self.polygon_data):
            content += f"**Polygon {i+1}:**\n"
            if len(polygon) > 0:
                for j, (x, y) in enumerate(polygon):
                    content += f"  Vertex {j+1}: ({x:.2f}, {y:.2f})\n"
            else:
                content += "  No vertices\n"
            content += "\n"

        return content

    def _on_polygon_change(self, data):
        """Callback for polygon data changes."""
        # Extract polygon data from the stream
        if data:
            polygons = []
            # Convert polygon array to list of (x, y) tuples
            for i in range(len(data['xs'])):
                vertices = [
                    (float(x), float(y))
                    for x, y in zip(data['xs'][i], data['ys'][i], strict=True)
                ]
                polygons.append(vertices)

            self.polygon_data = polygons
            # Update the display widget
            self._polygon_display.object = self._format_polygon_vertices()

    def create_detector_plot_content(self):
        """Create reactive detector plot content."""
        image_dmap = hv.DynamicMap(
            self._create_detector_image_plot, streams=[self._detector_image_pipe]
        )
        histogram_dmap = hv.DynamicMap(
            self._create_detector_histogram_plot,
            streams=[self._detector_histogram_pipe],
        )
        # Initial polygon data (scaled to image bounds)
        poly = hv.Polygons([[]])

        # Create PolyDraw stream
        self._poly_stream = streams.PolyDraw(
            source=poly,
            drag=True,
            num_objects=4,
            show_vertices=True,
            styles={'fill_color': ['red', 'green', 'blue', 'orange']},
        )

        # Connect polygon changes to callback
        self._poly_stream.add_subscriber(self._on_polygon_change)

        # Create PolyEdit stream for editing existing polygons
        self._poly_edit_stream = streams.PolyEdit(
            source=poly, vertex_style={'color': 'red'}, shared=True, show_vertices=True
        )

        # Also connect edit stream to callback
        self._poly_edit_stream.add_subscriber(self._on_polygon_change)

        # Configure polygon styling for visibility over image
        interactive_poly = poly.opts(
            opts.Polygons(
                fill_alpha=0.4,
                line_color='white',
                line_width=3,
                active_tools=['poly_draw', 'poly_edit'],
            )
        )

        # Overlay polygons on the image
        image_dmap = image_dmap * interactive_poly

        # Disable shared axes by setting shared_axes=False on each plot
        image_dmap = image_dmap.opts(shared_axes=False)
        histogram_dmap = histogram_dmap.opts(shared_axes=False)

        return pn.FlexBox(
            pn.pane.HoloViews(image_dmap), pn.pane.HoloViews(histogram_dmap)
        )

    def create_detector_plots(self):
        """Create plots for the Detectors tab with reactive content."""
        view_toggle = pn.widgets.RadioBoxGroup(
            name="View Mode",
            value=self.detector_view_mode,
            options=["Current", "Cumulative"],
            inline=True,
            margin=(10, 0),
        )

        view_toggle.link(self, value='detector_view_mode')

        return pn.Column(
            view_toggle,
            pn.Column(
                self.create_detector_plot_content(),
                scroll=True,
                height=300,
            ),
        )

    def create_monitor_plots(self) -> list:
        """Create plots for the Monitors tab."""
        timeseries_dmap = hv.DynamicMap(
            self._create_monitor_timeseries_plot,
            streams=[self._monitor_timeseries_pipe],
        )

        profile_dmap = hv.DynamicMap(
            self._create_monitor_profile_plot, streams=[self._monitor_profile_pipe]
        )

        return [pn.pane.HoloViews(timeseries_dmap), pn.pane.HoloViews(profile_dmap)]

    def create_reduction_plots(self) -> list:
        """Create plots for the Data Reduction tab."""
        comparison_dmap = hv.DynamicMap(
            self._create_reduction_comparison_plot,
            streams=[self._reduction_comparison_pipe],
        )

        residuals_dmap = hv.DynamicMap(
            self._create_reduction_residuals_plot,
            streams=[self._reduction_residuals_pipe],
        )

        return [pn.pane.HoloViews(comparison_dmap), pn.pane.HoloViews(residuals_dmap)]

    @pn.depends('active_tab')
    def get_dynamic_sidebar(self):
        """Create dynamic sidebar that updates based on active tab."""
        common_controls = pn.Column(
            pn.pane.Markdown("## Status"),
            pn.pane.Markdown("## Controls"),
            pn.Param(
                self,
                parameters=['sample_name', 'run_number'],
                show_name=False,
                width=250,
            ),
            pn.layout.Spacer(height=20),
        )

        if self.active_tab == "Detectors":
            specific_controls = pn.Column(
                pn.pane.Markdown("## Detector Settings"),
                pn.Param(
                    self,
                    parameters=['detector_threshold', 'pulse'],
                    show_name=False,
                    width=250,
                ),
                pn.layout.Spacer(height=20),
                self._polygon_display,
            )
        elif self.active_tab == "Monitors":
            specific_controls = pn.Column(
                pn.pane.Markdown("## Monitor Settings"),
                pn.Param(
                    self,
                    parameters=['monitor_integration_time', 'monitor_normalization'],
                    show_name=False,
                    width=250,
                ),
            )
        elif self.active_tab == "Data Reduction":
            specific_controls = pn.Column(
                pn.pane.Markdown("## Reduction Settings"),
                pn.Param(
                    self,
                    parameters=['reduction_algorithm', 'background_subtraction'],
                    show_name=False,
                    width=250,
                ),
            )
        else:
            specific_controls = pn.Column()

        return pn.Column(common_controls, specific_controls)


def create_dashboard():
    """Create and configure the main dashboard."""
    app = DashboardApp()

    detector_content = app.create_detector_plots()
    monitor_plots = pn.FlexBox(*app.create_monitor_plots())
    reduction_plots = pn.FlexBox(*app.create_reduction_plots())

    # Create tabs
    tabs = pn.Tabs(
        ("Detectors", detector_content),
        ("Monitors", monitor_plots),
        ("Data Reduction", reduction_plots),
        dynamic=True,
        sizing_mode='stretch_width',
    )

    # Callback to update active tab parameter
    def update_active_tab(event):
        tab_names = ["Detectors", "Monitors", "Data Reduction"]
        new_tab_name = tab_names[event.new]
        app.active_tab = new_tab_name

    tabs.param.watch(update_active_tab, 'active')

    # Configure template with dynamic sidebar
    template = pn.template.MaterialTemplate(
        title="DREAM — Live Data",
        sidebar=app.get_dynamic_sidebar,
        main=tabs,
        header_background='#2596be',
    )

    return template


def main():
    """Main function to serve the Panel dashboard."""
    dashboard = create_dashboard()
    dashboard.servable()
    return dashboard


if __name__ == "__main__":
    app = main()
    pn.serve(app, port=5007, show=False, autoreload=True, dev=True)
